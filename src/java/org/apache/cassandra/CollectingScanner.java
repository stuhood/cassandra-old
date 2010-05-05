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

package org.apache.cassandra;

import java.io.*;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Scanner;

import org.apache.cassandra.db.ColumnKey;

import com.google.common.collect.AbstractIterator;

/**
 * Garbage collects an underlying Scanner.
 */
public class CollectingScanner extends AbstractIterator<ASlice> implements Scanner
{
    private final Scanner scanner;
    private final ASlice.GCFunction gcfunc;

    protected CollectingScanner(Scanner scanner, int gcBefore)
    {
        assert gcBefore > Integer.MIN_VALUE : "CollectingScanner unnecessary.";
        this.scanner = scanner;
        this.gcfunc = new ASlice.GCFunction(gcBefore);
    }

    /**
     * Wraps garbage collection around the given Scanner, unless gcBefore indicates no collection should be performed.
     * @return A garbage collected Scanner or the identical input Scanner.
     */
    public static Scanner collect(Scanner scanner, int gcBefore)
    {
        if (gcBefore > Integer.MIN_VALUE)
            return new CollectingScanner(scanner, gcBefore);
        return scanner;
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
        while (scanner.hasNext())
        {
            ASlice slice = gcfunc.apply(scanner.next());
            if (slice == null)
                continue;
            return slice;
        }

        return endOfData();
    }
}
