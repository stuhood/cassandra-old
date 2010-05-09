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

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Slice;
import org.apache.cassandra.Scanner;

import org.apache.cassandra.db.ColumnKey;

import com.google.common.collect.AbstractIterator;

/**
 * Limits the number of columns that will be returned by an underlying Scanner.
 */
class LimitingScanner extends AbstractIterator<ASlice> implements Scanner
{
    private final Scanner scanner;
    private final long limit;
    private long accepted;

    public LimitingScanner(Scanner scanner, long limit)
    {
        assert limit < Long.MAX_VALUE : "LimitingScanner unnecessary.";
        this.scanner = scanner;
        this.limit = limit;
        accepted = 0;
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

    @Override
    public long getBytesRemaining()
    {
        // would be very difficult to calculate the number of unlimited bytes
        return scanner.getBytesRemaining();
    }

    @Override
    public ASlice computeNext()
    {
        if (!scanner.hasNext())
            return endOfData();
        long remainder = limit - accepted;
        if (remainder <= 0)
            // we can't accept any more columns
            return endOfData();

        // we can accept at least 1 column
        ASlice slice = scanner.next();
        if (remainder >= slice.count())
            // accept the entire slice
            accepted += slice.count();
        else
        {
            // accept only part of the slice
            int acceptable = (int)Math.min(remainder, slice.count());
            slice = new Slice(slice.meta, slice.begin, slice.end, slice.columns().subList(0, acceptable));
            accepted += acceptable;
        }
        return slice;
    }
}
