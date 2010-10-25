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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.io.ICompactionInfo;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Implements a secondary index for a column family using a second column family in which the row
 * keys are indexed values, and column names are base row keys.
 */
public class KeysIndex extends SecondaryIndex
{
    static final Logger logger = LoggerFactory.getLogger(KeysIndex.class);

    // the base column family store
    private final ColumnFamilyStore cfs;
    // the index column family store
    private final ColumnFamilyStore icfs;

    public KeysIndex(ColumnDefinition cdef, ColumnFamilyStore cfs)
    {
        super(cdef);
        this.cfs = cfs;
        AbstractType columnComparator = cfs.partitioner.equivalentType();
        CFMetaData icfm = CFMetaData.newIndexMetadata(cfs.table.name, cfs.columnFamily, cdef, columnComparator);
        this.icfs = ColumnFamilyStore.createColumnFamilyStore(cfs.table,
                                                              icfm.cfName,
                                                              new LocalPartitioner(cfs.metadata.getColumn_metadata().get(cdef.name).validator),
                                                              icfm);
    }

    @Override
    public void initialize()
    {
        if (SystemTable.isIndexBuilt(cfs.table.name, icfs.metadata.cfName))
            return;
        // record that the column is supposed to be indexed, before we start building it
        // (so we don't omit indexing writes that happen during build process)
        logger.info("Creating index {}.{}", cfs.table, icfs.metadata.cfName);
        try
        {
            cfs.forceBlockingFlush();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        cfs.rebuildSecondaryIndex(this, cfs.getSSTables());
        logger.info("Index {} complete", icfs.metadata.cfName);
        SystemTable.setIndexBuilt(cfs.table.name, icfs.metadata.cfName);
    }

    public void purge()
    {
        SystemTable.setIndexRemoved(cfs.metadata.tableName, cfs.metadata.cfName);
        icfs.removeAllSSTables();
    }

    public double selectivity(IndexExpression expr)
    {
        if (expr.op != IndexOperator.EQ)
            // TODO: currently only implementing EQ for keys indexes
            return Double.MAX_VALUE;
        // FIXME: using maxColumns will give a pretty terrible estimate: we need to get
        // a row count estimate directly from the base cfs
        return (double)icfs.getMeanColumns() / (1 + icfs.getMaxColumns());
    }

    public boolean certainty(IndexExpression expr)
    {
        return true;
    }

    public KeysIterator iterator(AbstractBounds range, IndexExpression expr, ByteBuffer startKey)
    {
        return new KeysIterator(icfs, cfs.partitioner, range, icfs.partitioner.decorateKey(expr.value), startKey);
    }

    public ColumnFamilyStore getIndexCFS()
    {
        return icfs;
    }

    /**
     * Rebuild all Keys indexes that were affected by the addition of the given sstables.
     */
    public static void rebuild(ColumnFamilyStore cfs, Collection<SecondaryIndex> indexes, Collection<SSTableReader> sstables)
    {
        // collect keys indexes
        TreeSet<ByteBuffer> names = new TreeSet<ByteBuffer>(cfs.metadata.comparator);
        List<SecondaryIndex> kindexes = new ArrayList<SecondaryIndex>();
        for (SecondaryIndex index : indexes)
        {
            if (index.cdef.getIndexType() != IndexType.KEYS)
                continue;
            names.add(index.cdef.name);
            kindexes.add(index);
        }
        if (kindexes.isEmpty())
            // no keys indexes
            return;

        // rebuild each index for all affected keys
        Table.KeysIndexBuilder builder = cfs.table.createIndexBuilder(cfs, names, new ReducingKeyIterator(sstables));
        logger.debug("Submitting index build to compactionmanager for {}", builder);
        Future future = CompactionManager.instance.submitIndexBuild(cfs, builder);
        try
        {
            future.get();
            for (SecondaryIndex kindex : kindexes)
                kindex.getIndexCFS().forceBlockingFlush();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
