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

import org.apache.commons.collections.iterators.CollatingIterator;
import com.google.common.base.Predicates;

import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterIterator;
import org.apache.cassandra.utils.ReducingIterator;

/**
 * Implements a binned bitmap secondary index, where individual sstables are indexed
 * independently, and the results are joined from the memtable and each sstable at
 * query time. Returns keys that match with high probability, but will often return
 * false positives.
 */
public class KeysBitmapIndex extends SecondaryIndex
{
    // the column family store that is being indexed
    private final ColumnFamilyStore cfs;

    public KeysBitmapIndex(ColumnFamilyStore cfs)
    {
        super();
        this.cfs = cfs;
    }

    public IndexType type()
    {
        return IndexType.KEYS_BITMAP;
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
