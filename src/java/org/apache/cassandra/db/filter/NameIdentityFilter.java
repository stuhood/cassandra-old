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

import java.util.Comparator;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.ColumnKey;

class NameIdentityFilter implements IFilter<byte[]>
{
    private final static NameIdentityFilter SINGLETON = new NameIdentityFilter();

    private NameIdentityFilter() {}

    /**
     * @return The NameIdentityFilter singleton.
     */
    public static NameIdentityFilter get()
    {
        return SINGLETON;
    }

    @Override
    public byte[] initial()
    {
        return ColumnKey.NAME_BEGIN;
    }

    @Override
    public MatchResult<byte[]> matchesBetween(byte[] begin, byte[] end)
    {
        return MatchResult.MATCH_CONT;
    }

    @Override
    public boolean matches(byte[] name)
    {
        return true;
    }
}
