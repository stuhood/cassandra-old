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

package org.apache.cassandra.db;

import java.io.*;
import java.util.*;

import org.apache.cassandra.AScanner;
import org.apache.cassandra.ASlice;
import org.apache.cassandra.Scanner;

import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryFilter;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A simple Scanner over a list of Slices.
 */
public class ListScanner extends AScanner
{
    private final List<ASlice> slices;
    // the current position in the list
    private Iterator<ASlice> iter = null;

    public ListScanner(List<ASlice> slices, ColumnKey.Comparator comparator)
    {
        super(comparator);
        this.slices = slices;
    }

    @Override
    public boolean first()
    {
        iter = slices.iterator();
        return iter.hasNext();
    }

    @Override
    public boolean seekNear(ColumnKey seekKey)
    {
        if (!first())
            return false;

        PeekingIterator<ASlice> piter = Iterators.peekingIterator(iter);
        iter = piter;
        // linear search while we're positioned before the given key
        while(piter.hasNext() && comparator.compare(piter.peek().end, seekKey) < 0)
            piter.next();
        return piter.hasNext();
    }

    public boolean hasNext()
    {
        if (iter == null)
            return first();
        return iter.hasNext();
    }

    public ASlice next()
    {
        if (iter == null)
            first();
        return filter(iter.next());
    }

    @Override
    public void close()
    {
        iter = null;
    }
}
