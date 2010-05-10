package org.apache.cassandra.db.filter;
/*
 * 
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
 * 
 */


import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.collections.iterators.ReverseListIterator;
import org.apache.commons.collections.IteratorUtils;

import com.google.common.collect.Collections2;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

public class NameSliceFilter implements IFilter<byte[]>
{
    private static Logger logger = LoggerFactory.getLogger(NameSliceFilter.class);

    private final Comparator<byte[]> comp;
    public final byte[] start;
    public final byte[] finish;
    public final List<byte[]> bitmasks;
    public final boolean reversed;

    public NameSliceFilter(Comparator<byte[]> comp, byte[] start, byte[] finish, List<byte[]> bitmasks, boolean reversed)
    {
        this.comp = comp;
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.bitmasks = bitmasks;
    }

    /**
     * Performs non-wrapping intersection: column name slices are (inclusive, inclusive).
     */
    @Override
    public MatchResult<byte[]> matchesBetween(byte[] begin, byte[] end)
    {
        if (comp.compare(end, start) < 0)
            // we are before our interesting slice: seek forward
            return MatchResult.<byte[]>get(false, MatchResult.OP_SEEK, start);
        if (comp.compare(finish, begin) < 0 && finish.length != 0)
            // we are after our interesting slice: indicate that we are finished
            return MatchResult.NOMATCH_DONE;
        return MatchResult.MATCH_CONT;
    }

    @Override
    public boolean matches(byte[] name)
    {
        if (bitmasks != null && !bitmasks.isEmpty() && !matchesBitmasks(name))
            return false;

        // contains
        // TODO: replace finish.length check with ColumnKey.NAME_END when all
        // components are using Slices
        return comp.compare(start, name) <= 0 && (finish.length == 0 || comp.compare(name, finish) <= 0);
    }
    
    public boolean matchesBitmasks(byte[] name)
    {
        for (byte[] bitmask : bitmasks)
            if (matchesBitmask(bitmask, name))
                return true;
        return false;
    }

    public static boolean matchesBitmask(byte[] bitmask, byte[] name)
    {
        assert name != null;
        assert bitmask != null;

        int len = Math.min(bitmask.length, name.length);

        for (int i = 0; i < len; i++)
        {
            if ((bitmask[i] & name[i]) == 0)
            {
                return false;
            }
        }

        return true;
    }
}
