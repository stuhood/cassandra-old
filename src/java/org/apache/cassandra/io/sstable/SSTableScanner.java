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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IColumnIterator;
import org.apache.cassandra.io.Scanner;
import org.apache.cassandra.io.Slice;
import org.apache.cassandra.io.SliceBuffer;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ReducingIterator;

import com.google.common.collect.*;

public interface SSTableScanner extends Scanner
{
    /**
     * @return The underlying SSTableReader.
     */
    public SSTableReader reader();

    /**
     * See the contract for seekNear(CK).
     */
    public boolean seekNear(DecoratedKey seekKey) throws IOException;

    /**
     * Seeks to the slice which might contain the given key, without checking the
     * existence of the key in the filter. If such a slice does not exist, the next
     * calls to next() will have undefined results.
     *
     * seekKeys with trailing NAME_BEGIN or NAME_END names will properly match
     * the slices that they begin or end when used with this method.
     *
     * @return False if no such Slice was found.
     */
    public boolean seekNear(ColumnKey seekKey) throws IOException;

    /**
     * See the contract for seekTo(CK).
     */
    public boolean seekTo(DecoratedKey seekKey) throws IOException;

    /**
     * Seeks to the slice which might contain the given key. If the key does not
     * exist, or such a slice does not exist, the next calls to next() will have
     * undefined results.
     *
     * seekKeys with trailing NAME_BEGIN or NAME_END names will properly match
     * the slices that they begin or end when used with this method.
     *
     * @return False if no such Slice was found.
     */
    public boolean seekTo(ColumnKey seekKey) throws IOException;

    /**
     * @return True if the scanner has been successfully positioned, and contains
     * at least one more Slice.
     */
    public boolean hasNext();

    /**
     * @return The next slice (preferably still in serialized form), unless the last call to seek*() failed, or we are at
     * the end of the file.
     */
    public SliceBuffer next();
}
