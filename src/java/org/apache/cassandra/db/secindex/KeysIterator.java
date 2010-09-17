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
import java.util.ArrayDeque;
import java.util.NoSuchElementException;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Iterates over keys matched by a particular index expression for an IndexType.KEYS index.
 */
public class KeysIterator implements CloseableIterator<DecoratedKey>
{
    private static Logger logger = LoggerFactory.getLogger(KeysIterator.class);

    public static final int BUFFER_COUNT = 128;

    private final ColumnFamilyStore indexCFS;
    private final IPartitioner basep;
    private final AbstractBounds range;
    private final DecoratedKey indexKey;
    private final QueryPath path;

    // beginning of the current index block
    private ByteBuffer startKey;
    // current index block, or null
    private ArrayDeque<DecoratedKey> buffer;

    public KeysIterator(ColumnFamilyStore indexCFS, IPartitioner basep, AbstractBounds range, DecoratedKey indexKey, ByteBuffer startKey)
    {
        this.indexCFS = indexCFS;
        this.basep = basep;
        this.path = new QueryPath(indexCFS.columnFamily);
        this.range = range;
        this.indexKey = indexKey;
        this.startKey = startKey;
        this.buffer = new ArrayDeque<DecoratedKey>();
    }

    public boolean hasNext()
    {
        if (!buffer.isEmpty())
            // we have a buffered block
            return true;
        if (startKey == null)
            // nothing left to scan
            return false;
        
        while (startKey != null)
        {
            // query for the next block
            if (logger.isDebugEnabled())
                logger.debug(String.format("Scanning index row %s:%s starting with %s",
                                           indexCFS.columnFamily, indexKey, indexCFS.getComparator().getString(startKey)));
            QueryFilter indexFilter = QueryFilter.getSliceFilter(indexKey,
                                                                 path,
                                                                 startKey,
                                                                 FBUtilities.EMPTY_BYTE_BUFFER,
                                                                 false,
                                                                 BUFFER_COUNT);
            ColumnFamily block = indexCFS.getColumnFamily(indexFilter);
            logger.debug("fetched {}", block);
            if (block == null)
                return false;

            int colsInBlock = 0;
            for (IColumn column : block.getSortedColumns())
            {
                startKey = column.name();
                if (++colsInBlock == BUFFER_COUNT)
                    // exclude this key, and perform another slice
                    break;
                if (column.isMarkedForDelete())
                    continue;
                DecoratedKey dk = basep.decorateKey(column.name());
                /* we don't have a way to get the key back from the range -- we just have a token --
                 * so, we need to loop after starting with start_key, until we get to keys in the given `range`.
                 * But, if the calling StorageProxy is doing a good job estimating data from each range, the range
                 * should be pretty close to `start_key`. */
                if (!range.right.equals(basep.getMinimumToken()) && range.right.compareTo(dk.token) < 0)
                    // after the end of the range
                    break;
                if (!range.contains(dk.token))
                    // before the beginning of the range
                    continue;

                // matched a key: add to the buffer
                buffer.add(dk);
            }
            if (colsInBlock < BUFFER_COUNT)
                // no more slices
                startKey = null;
            if (!buffer.isEmpty())
                // found some keys
                break;
        }
        return !buffer.isEmpty();
    }

    public DecoratedKey next()
    {
        if (!hasNext()) throw new NoSuchElementException();
        return buffer.pollFirst();
    }

    public void close() { /* pass */ }
    public void remove() { throw new UnsupportedOperationException(); }
}
