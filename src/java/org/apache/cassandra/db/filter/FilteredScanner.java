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

import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.QueryFilter;

import com.google.common.collect.AbstractIterator;

/**
 * Filters a Scanner using a collection of non-intersecting ranges.
 */
public class FilteredScanner extends AbstractIterator<ASlice> implements Scanner
{
    private final SeekableScanner scanner;
    private final QueryFilter filter;

    // the result of our previous filtering attempt: when null, we have not begun scanning
    private MatchResult<ColumnKey> result;

    public FilteredScanner(SeekableScanner scanner, QueryFilter filter)
    {
        this.scanner = scanner;
        this.filter = filter;

        // push down column level filtering: slices we receive will contain only matching columns
        this.scanner.pushdownFilter(filter.nameFilter(scanner.comparator().columnDepth()));
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
     * Tries to perform the suggested 'hint' from our last MatchResult.
     * @return True if taking the hint resulted in our being positioned at a slice that might match.
     */
    private boolean takeHint()
    {
        if (result == null)
            // no hints available: be naive
            // TODO: before the first match, the filter should still be able to hint at where to seek first
            return scanner.hasNext();

        switch (result.hint)
        {
            case MatchResult.OP_DONE:
                // no more matches to be found
                return false;
            case MatchResult.OP_CONT:
                // the next slice might be interesting
                return scanner.hasNext();
            case MatchResult.OP_SEEK:
            {
                // seek to the next possible match
                try
                {
                    return scanner.seekNear(result.seekkey);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
            default:
                throw new RuntimeException("Unknown MatchResult hint code: " + result.hint);
        }
    }

    /**
     * Checks whether the current slice matches our embedded filter, and follows hints returned by the
     * filter to help find the next matching slice.
     *
     * @return The next slice matching the filter.
     */
    @Override
    public ASlice computeNext()
    {
        while (takeHint())
        {
            ASlice slice = scanner.next();
            result = filter.matches(slice.begin, slice.end);
            if (result.matched)
                // slice matched the filter: the hint will be stored for the next call to computeNext
                return slice;
        }
        
        return endOfData();
    }
}
