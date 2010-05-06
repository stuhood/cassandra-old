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

package org.apache.cassandra.db.filter;

import java.io.*;
import java.util.*;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Scanner;
import org.apache.cassandra.SeekableScanner;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import com.google.common.collect.AbstractIterator;

/**
 * Filters a Scanner using a list of non-intersecting ranges.
 */
public class FilteredScanner extends AbstractIterator<ASlice> implements Scanner
{
    private final SeekableScanner scanner;
    private final ArrayList<Range> ranges;
    private int rangeidx;

    public FilteredScanner(SeekableScanner scanner, Collection<Range> ranges) throws IOException
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
    }

    @Override
    public ColumnKey.Comparator comparator()
    {
        return scanner.comparator();
    }

    @Override
    public void close() throws IOException
    {
        scanner.close();
    }

    public long getBytesRemaining()
    {
        // would be very difficult to calculate the number of unfiltered bytes
        return scanner.getBytesRemaining();
    }

    /**
     * Checks whether the current range matches the current slice. If not, skips
     * forward through the ranges and slices looking for the next possible match.
     *
     * @return The next slice matching the filter.
     */
    @Override
    public ASlice computeNext()
    {
        try
        {
            // skip through ranges/slices until we find a slice contained in a range
            while (scanner.hasNext() && rangeidx < ranges.size())
            {
                ASlice slice = scanner.next();
                if (ranges.get(rangeidx).contains(slice.begin.dk.token))
                    // range contains slice
                    return slice;

                if (ranges.get(rangeidx).left.compareTo(slice.begin.dk.token) > 0)
                {
                    // slice is less than current range: seek to the nearest slice
                    if (!scanner.seekNear(new DecoratedKey(ranges.get(rangeidx).left)))
                        // no more data anywhere after the current range
                        break;
                }
                else
                {
                    // range is less than slice: skip to next range
                    rangeidx++;
                }
            }
        }
        catch(IOException e)
        {
            throw new IOError(e);
        }
        
        return endOfData();
    }
}
