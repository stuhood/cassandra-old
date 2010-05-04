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

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.INameFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.Scanner;
import org.apache.cassandra.io.SliceBuffer;
import org.apache.cassandra.service.StorageService;

import com.google.common.collect.AbstractIterator;

/**
 * Filters a Scanner using a collection of non-intersecting ranges.
 */
public class FilteredScanner extends AbstractIterator<SliceBuffer> implements Scanner
{
    private final Scanner scanner;
    private final QueryFilter filter;

    public FilteredScanner(Scanner scanner, QueryFilter filter)
    {
        this.scanner = scanner;
        this.filter = filter;

        // push down column level filtering: slices we receive will contain only matching columns
        this.scanner.pushdownFilter(filter.nameFilter(scanner.columnDepth()));
    }

    /**
     * TODO: should return a key to indicate where to seek next, giving the effect of a coroutine.
     * For instance, in the KeyIdentityFilter case, we always want to seek to the end of the given key,
     * possibly with additional seek information from the NameFilters.
     *
     * @return True if the slice represented by the given ColumnKeys might contain matching columns.
     */
    public boolean matches(ColumnKey begin, ColumnKey end)
    {
        if (!filter.keyFilter().matchesKey(begin.dk))
            return false;
        for (int i = 1; i <= columnDepth(); i++)
        {
            if (!filter.nameFilter(i).mightMatchSlice(comparator().comparatorAt(i), begin.name(i), end.name(i)))
                return false;
        }
        return true;
    }

    @Override
    public int columnDepth()
    {
        return scanner.columnDepth();
    }

    @Override
    public ColumnKey.Comparator comparator()
    {
        return scanner.comparator();
    }

    @Override
    public void pushdownFilter(INameFilter filter)
    {
        // a filtered scanner is already too busy applying its own filters
        throw new RuntimeException("Filtering a FilteredScanner is redundant!");
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
    public boolean first()
    {
        if (!scanner.first())
            return false;
        return hasNext();
    }

    @Override
    public boolean seekNear(DecoratedKey seekKey) throws IOException
    {
        // TODO: laziness
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean seekNear(ColumnKey seekKey) throws IOException
    {
        // TODO: laziness
        throw new RuntimeException("Not implemented");
    }

    /**
     * Checks whether the current slice matches our embedded filter. If not, skips
     * forward through slices looking for the next possible match.
     *
     * FIXME: Totally naive at the moment: filters individual slices, and never seeks. See the comments in
     * QueryFilter for "the plan".
     *
     * @return The next slice matching the filter.
     */
    @Override
    public SliceBuffer computeNext()
    {
        while (scanner.hasNext())
        {
            SliceBuffer slice = scanner.next();
            if (matches(slice.begin, slice.end))
                return slice;
        }
        
        return endOfData();
    }
}
