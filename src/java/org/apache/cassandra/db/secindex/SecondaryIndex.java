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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Abstract base class for different types of secondary indexes.
 */
public abstract class SecondaryIndex
{
    public final ColumnDefinition cdef;
    public SecondaryIndex(ColumnDefinition cdef)
    {
        this.cdef = cdef;
    }

    public static SecondaryIndex open(ColumnDefinition info, ColumnFamilyStore cfs)
    {
        SecondaryIndex idx;
        if (info.getIndexType() == IndexType.KEYS)
            idx = new KeysIndex(info, cfs);
        else
        {
            assert info.getIndexType() == IndexType.KEYS_BITMAP;
            idx = new KeysBitmapIndex(info, cfs);
        }
        idx.initialize();
        return idx;
    }

    /**
     * Called after construction: default impl is a noop.
     */
    public void initialize() {}

    /**
     * Called when this index is no longer necessary, and persisted data should be
     * removed from disk.
     */
    public abstract void purge();

    /**
     * @return The fraction of rows matched by the given expression for this index, or
     * Double.MAX_VALUE if this index cannot be used.
     */
    public abstract double selectivity(IndexExpression expr);

    /**
     * @return True if keys returned by an iterator for the given expression are certain matches,
     * as opposed to probable matches.
     */
    public abstract boolean certainty(IndexExpression expr);

    /**
     * @return An iterator over keys that match the given expression with certainty().
     */
    public abstract CloseableIterator<DecoratedKey> iterator(AbstractBounds range, IndexExpression expr, ByteBuffer startKey);

    /**
     * Leaky abstraction.
     * @return The private ColumnFamilyStore storing this index, or null.
     */
    public abstract ColumnFamilyStore getIndexCFS();

    public static boolean satisfies(ColumnFamily data, List<IndexExpression> expressions)
    {
        for (IndexExpression expression : expressions)
        {
            // check column data vs expression
            IColumn column = data.getColumn(expression.column_name);
            if (column == null)
                return false;
            int v = data.getComparator().compare(column.value(), expression.value);
            if (!satisfies(v, expression.op))
                return false;
        }
        return true;
    }

    static boolean satisfies(int comparison, IndexOperator op)
    {
        switch (op)
        {
            case EQ:
                return comparison == 0;
            case GTE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LTE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                throw new IllegalStateException();
        }
    }
}
