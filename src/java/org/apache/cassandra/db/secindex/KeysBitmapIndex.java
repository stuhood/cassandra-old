/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.secindex;

import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.collections.iterators.CollatingIterator;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.bitidx.BitmapIndexReader;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ReducingIterator;

/**
 * Implements a binned bitmap secondary index, where individual sstables are indexed
 * independently, and the results are joined from the memtable and each sstable at
 * query time. Returns keys that match with high probability, but will often return
 * false positives.
 */
public class KeysBitmapIndex extends SecondaryIndex
{
    static final Logger logger = LoggerFactory.getLogger(KeysBitmapIndex.class);

    // the column family store that is being indexed
    private final ColumnFamilyStore cfs;

    public KeysBitmapIndex(ColumnDefinition cdef, ColumnFamilyStore cfs)
    {
        super(cdef);
        this.cfs = cfs;
    }

    public double selectivity(IndexExpression expr)
    {
        if (cfs.getSSTables().isEmpty())
            // no sstables: no disk io (TODO: consider races)
            return 0;
        // take the mean selectivity of all sstables for the expression
        long totalSelected = 0;
        long totalExisting = 1;
        for (SSTableReader reader : cfs.getSSTables())
        {
            totalSelected += reader.cardinality(expr);
            totalExisting += reader.estimatedKeys();
        }
        return ((double)totalSelected) / totalExisting;
    }

    public boolean certainty(IndexExpression expr)
    {
        // TODO: there are a tiny few cases where a binned bitmap index can be certain
        return false;
    }

    public CloseableIterator<DecoratedKey> iterator(AbstractBounds range, IndexExpression expr, ByteBuffer startKey)
    {
        DecoratedKey dk = cfs.partitioner.decorateKey(startKey);
        // FIXME: memtable should filter inside the bounds
        CloseableIterator<DecoratedKey> memiter = new FilterIterator(cfs.filterMemtables(expr, dk), Predicates.notNull());
        if (cfs.getSSTables().isEmpty())
            // no active sstables
            return memiter;

        // merge probably matching keys from the memtables and sstables
        List<CloseableIterator<DecoratedKey>> iters = new ArrayList<CloseableIterator<DecoratedKey>>();
        iters.add(memiter);
        for (SSTableReader sstable : cfs.getSSTables())
            iters.add(sstable.scan(expr, range));
        return new ProbableKeyIterator(iters);
    }

    public ColumnFamilyStore getIndexCFS()
    {
        return null;
    }

    /**
     * Syncs the in-memory index definitions with indexes on disk for the given SSTables: modifies the sstable map.
     * TODO: could be generalized into recovery for any missing components
     */
    public static Map<Descriptor,Set<Component>> preload(ColumnFamilyStore cfs, Map<Descriptor,Set<Component>> sstables) throws IOException
    {
        Collection<ColumnDefinition> defs = cfs.metadata.getColumn_metadata().values();

        // collect indexes of type KeysBitmap
        Set<ColumnDefinition> keysb = new HashSet<ColumnDefinition>();
        for (ColumnDefinition def : defs)
            if (def.getIndexType() == IndexType.KEYS_BITMAP)
                keysb.add(def);
        if (keysb.isEmpty())
            // nothing to sync
            return sstables;
        
        // recover the indexes for any sstables that don't have them
        Map<Descriptor,Set<Component>> output = new HashMap<Descriptor,Set<Component>>();
        for (Map.Entry<Descriptor,Set<Component>> sstable : sstables.entrySet())
        {
            // load definitions for indexes on disk
            Map<ColumnDefinition,Component> ondisk = new HashMap<ColumnDefinition,Component>();
            for (Component component : sstable.getValue())
                if (component.type == Component.Type.BITMAP_INDEX)
                    ondisk.put(BitmapIndexReader.metadata(sstable.getKey(), component), component);

            if (ondisk.keySet().equals(keysb))
            {
                // on-disk and in-memory match for this sstable
                output.put(sstable.getKey(), sstable.getValue());
                continue;
            }

            // drop indexes that shouldn't be on disk
            for (Map.Entry<ColumnDefinition,Component> index : ondisk.entrySet())
            {
                if (defs.contains(index.getKey()))
                    // valid index
                    continue;
                // old index: remove
                sstable.getValue().remove(index.getValue());
                FileUtils.deleteWithConfirm(sstable.getKey().filenameFor(index.getValue()));
            }

            // if any indexes are missing from disk, rebuild the sstable
            if (Sets.difference(keysb, ondisk.keySet()).isEmpty())
                continue;
            
            // hardlink to a tmp copy (without any bitmap indexes)
            Descriptor clone = cfs.getTempSSTable(sstable.getKey().directory);
            Set<Component> ctoclone = Sets.difference(sstable.getValue(), new HashSet<Component>(ondisk.values()));
            SSTable.clone(sstable.getKey(), clone, ctoclone);

            // rebuild all bitmap indexes
            logger.info("Rebuilding index(es) on " + sstable.getKey() + " in " + clone);
            try
            {
                Pair<Descriptor,Set<Component>> rebuilt = CompactionManager.instance.submitSSTableBuild(cfs, clone, EnumSet.of(Component.Type.BITMAP_INDEX)).get();
                output.put(rebuilt.left, rebuilt.right);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to rebuild index(es) in " + clone, e);
            }

            // the rebuilt clone is now valid: remove the old sstable
            SSTable.markCompacted(sstable.getKey(), sstable.getValue());
            SSTable.delete(sstable.getKey(), sstable.getValue());
        }
        return output;
    }

    static final class ProbableKeyIterator extends ReducingIterator<DecoratedKey,DecoratedKey> implements CloseableIterator<DecoratedKey>
    {
        private final List<CloseableIterator<DecoratedKey>> sources;
        private DecoratedKey cur;
        public ProbableKeyIterator(List<CloseableIterator<DecoratedKey>> sources)
        {
            super(getCollatingIterator(sources));
            this.sources = sources;
        }

        private static CollatingIterator getCollatingIterator(List<CloseableIterator<DecoratedKey>> sources)
        {
            CollatingIterator citer = FBUtilities.<DecoratedKey>getCollatingIterator();
            for (CloseableIterator<DecoratedKey> source : sources)
                citer.addIterator(source);
            return citer;
        }

        @Override
        public void reduce(DecoratedKey inc)
        {
            cur = inc;
        }

        @Override
        protected DecoratedKey getReduced()
        {
            return cur;
        }

        public void close()
        {
            for (CloseableIterator source : sources) source.close();
        }
    }
}
