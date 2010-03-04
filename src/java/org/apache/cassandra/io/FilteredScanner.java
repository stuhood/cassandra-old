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

package org.apache.cassandra.io;

import java.io.*;
import java.util.*;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

/**
 * Filters a scanner using a list of non-intersecting ranges.
 */
public class FilteredScanner implements Scanner
{
    private final Scanner scanner;
    private final ArrayList<Range> ranges;
    private int rangeidx;
    private boolean valid;

    public FilteredScanner(Scanner scanner, Collection<Range> ranges) throws IOException
    {
        this.scanner = scanner;
        this.ranges = new ArrayList<Range>(ranges);
        Collections.sort(this.ranges);
        rangeidx = 0;
        
        // if one of the ranges was a wrapping range, it will be sorted first
        if (this.ranges.get(0).isWrapAround())
        {
            // split the wrapping range in two
            Range wrap = this.ranges.get(0);
            Token mintoken = StorageService.getPartitioner().getMinimumToken();
            this.ranges.add(new Range(wrap.left, mintoken));
            this.ranges.set(0, new Range(mintoken, wrap.right));
        }
        valid = false;
    }

    /**
     * Checks whether the current range matches the current slice. If not, skips
     * forward through the ranges and slices looking for the next possible match.
     *
     * @return True if we are positioned at a slice that matches the filter.
     */
    private boolean matchSlice() throws IOException
    {
        // skip through ranges/slices until we find a range containing a slice
        for (; rangeidx < ranges.size(); rangeidx++)
        {
            if (ranges.get(rangeidx).contains(scanner.get().begin.dk.token))
            {
                // range contains slice
                valid = true;
                return true;
            }
            // see if we should skip this range
            if (ranges.get(rangeidx).left.compareTo(scanner.get().begin.dk.token) > 0)
            {
                // slice is less than current range: seek to the next slice
                if (!scanner.seekNear(new DecoratedKey(ranges.get(rangeidx).left)))
                    // no more data for the current slice
                    break;
            }
        }
        valid = false;
        return false;
    }

    public void close() throws IOException
    {
        scanner.close();
    }

    public long getBytesRemaining()
    {
        // would be very difficult to calculate the number of unfiltered bytes
        return scanner.getBytesRemaining();
    }

    @Override
    public boolean first() throws IOException
    {
        // seek to the beginning of the first range
        rangeidx = 0;
        if (!scanner.seekNear(new DecoratedKey(ranges.get(rangeidx).left)))
        {
            valid = false;
            return false;
        }

        return matchSlice();
    }

    @Override
    public boolean seekNear(DecoratedKey seekKey) throws IOException
    {
        // laziness: compaction won't need to seek
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean seekNear(ColumnKey seekKey) throws IOException
    {
        // laziness: compaction won't need to seek
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean next() throws IOException
    {
        if (!scanner.next())
        {
            valid = false;
            return false;
        }
        
        return matchSlice();
    }

    @Override
    public Slice get()
    {
        if (!valid)
            return null;
        return scanner.get();
    }

    @Override
    public List<Column> getColumns() throws IOException
    {
        if (!valid)
            return null;
        return scanner.getColumns();
    }

    @Override
    public SliceBuffer getBuffer() throws IOException
    {
        if (!valid)
            return null;
        return scanner.getBuffer();
    }
}
