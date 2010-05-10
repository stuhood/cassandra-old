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

import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

public class NameListFilter implements IFilter<byte[]>
{
    private final Comparator<byte[]> comp;
    public final SortedSet<byte[]> columns;

    public NameListFilter(Comparator<byte[]> comp, Collection<byte[]> columns)
    {
        this.comp = comp;
        this.columns = new TreeSet<byte[]>(comp);
        this.columns.addAll(columns);
    }

    @Override
    public MatchResult<byte[]> matchesBetween(byte[] begin, byte[] end)
    {
        // TODO: could be made _much_ more efficient using sorted set properties
        for (byte[] name : columns)
            if (comp.compare(begin, name) <= 0 && comp.compare(name, end) <= 0)
                return MatchResult.MATCH_CONT;
        return MatchResult.NOMATCH_CONT;
    }

    @Override
    public boolean matches(byte[] name)
    {
        return columns.contains(name);
    }

    @Override
    public String toString()
    {
        return "#<NameListFilter " + columns + ">";
    }
}
