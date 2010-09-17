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

import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;

/**
 * Implements a secondary index for a column family using a second column family in which the row
 * keys are indexed values, and column names are base row keys.
 */
public class KeysIndex extends SecondaryIndex
{
    // the partitioner for the base data
    private final IPartitioner basep;
    // the index column family store
    private final ColumnFamilyStore icfs;

    public KeysIndex(ColumnFamilyStore icfs, IPartitioner basep)
    {
        super();
        this.icfs = icfs;
        this.basep = basep;
    }

    public IndexType type()
    {
        return IndexType.KEYS;
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
        return new KeysIterator(icfs, basep, range, icfs.partitioner.decorateKey(expr.value), startKey);
    }

    public ColumnFamilyStore getIndexCFS()
    {
        return icfs;
    }
}
