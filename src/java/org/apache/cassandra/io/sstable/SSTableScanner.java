/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.io.sstable;

import java.io.*;
import java.util.*;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.IteratingRow;
import org.apache.cassandra.io.Slice;
import org.apache.cassandra.io.SliceBuffer;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.*;

/**
 * A Scanner is an abstraction for reading slices from an SSTable.
 *
 * After creation, the Scanner is not positioned at a slice. Call first() or
 * seek*() to position it at a Slice, and then use next() to iterate.
 */
public abstract class SSTableScanner implements Closeable
{
    protected SSTableScanner()
    {
    }

    /**
     * @return The Comparator for this SSTable.
     */
    public int columnDepth()
    {
        return reader().getColumnDepth();
    }

    /**
     * @return The Comparator for this SSTable.
     */
    public ColumnKey.Comparator comparator()
    {
        return reader().getComparator();
    }

    /**
     * @return The underlying SSTableReader.
     */
    public abstract SSTableReader reader();

    /**
     * Releases any resources associated with this scanner.
     */
    public abstract void close() throws IOException;

    /**
     * @return The total length of the data file.
     */
    public abstract long getFileLength();

    /**
     * @return The approximate (due to buffering) position in the data file.
     */
    public abstract long getFilePointer();

    /**
     * Positions the Scanner at the first slice in the file. SSTables should never
     * be empty, so this call should always succeed.
     */
    public abstract void first() throws IOException;

    /**
     * See the contract for seekNear(CK).
     */
    public abstract boolean seekNear(DecoratedKey seekKey) throws IOException;

    /**
     * Seeks to the slice which might contain the given key, without checking the
     * existence of the key in the filter. If such a slice does not exist, the next
     * calls to get*() will have undefined results.
     *
     * seekKeys with trailing NAME_BEGIN or NAME_END names will properly match
     * the slices that they begin or end when used with this method.
     *
     * @return False if no such Slice was found.
     */
    public abstract boolean seekNear(ColumnKey seekKey) throws IOException;

    /**
     * See the contract for seekTo(CK).
     */
    public abstract boolean seekTo(DecoratedKey seekKey) throws IOException;

    /**
     * Seeks to the slice which might contain the given key. If the key does not
     * exist, or such a slice does not exist, the next calls to get*() will have
     * undefined results.
     *
     * seekKeys with trailing NAME_BEGIN or NAME_END names will properly match
     * the slices that they begin or end when used with this method.
     *
     * @return False if no such Slice was found.
     */
    public abstract boolean seekTo(ColumnKey seekKey) throws IOException;

    /**
     * @return True if we are positioned at a valid slice and a call to next() will be
     * successful.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * Seeks to the next slice, unless the last call to seek*() failed, or we are at
     * the end of the file.
     * @return True if we are positioned at the next valid slice.
     */
    public abstract boolean next() throws IOException;

    /**
     * @return The Slice at our current position, or null if the last call to
     * seekTo failed, or we're at EOF.
     */
    public abstract Slice get();

    /**
     * A list of columns contained in this slice. A slice may be a tombstone,
     * which only exists to pass along deletion Metadata, in which case the list
     * will be empty.
     *
     * @return A column list for the slice at our current position, or null if
     * the last call to seek*() failed, or we're at EOF.
     */
    public abstract List<Column> getColumns() throws IOException;

    /**
     * A buffer containing the columns in this slice, preferably still in serialized
     * form.
     *
     * @return A SliceBuffer for the slice at our current position, or null if
     * the last call to seek*() failed, or we're at EOF.
     */
    public abstract SliceBuffer getBuffer() throws IOException;

    /**
     * Reads the entire ColumnFamily defined by the key of the current Slice. After
     * the call, the Scanner will be positioned at the beginning of the first Slice
     * for the next ColumnFamily, or at EOF.
     *
     * FIXME: This is here temporarily as we port callers to the Slice API: using this
     * method in conjunction with next(), get() and friends will definitely not
     * work as expected.
     *
     * @return An IteratingRow for the current ColumnFamily, or null if get() would
     * return null.
     */
    @Deprecated
    public abstract IteratingRow getIteratingRow() throws IOException;

    /**
     * FIXME: This is here temporarily as we port callers to the Slice API.
     *
     * @return An iterator wrapping this Scanner and starting at the current position
     * of the Scanner.
     */
    @Deprecated
    public RowIterator getIterator()
    {
        return new RowIterator(this);
    }

    /**
     * FIXME: This is here temporarily as we port callers to the Slice API.
     */
    @Deprecated
    public static class RowIterator extends AbstractIterator<IteratingRow> implements PeekingIterator<IteratingRow>, Closeable
    {
        public final SSTableScanner scanner;
        RowIterator(SSTableScanner scanner)
        {
            this.scanner = scanner;
        }

        @Override
        protected IteratingRow computeNext()
        {
            try
            {
                IteratingRow row = scanner.getIteratingRow();
                if (row == null)
                    return endOfData();
                return row;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
        
        public void close() throws IOException
        {
            scanner.close();
        }
    }
}
