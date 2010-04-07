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

package org.apache.cassandra;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Metadata;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.Named;

/**
 * An immutable object which extends ASlice to add columns.
 */
public class Slice extends ASlice
{
    private final List<Column> columns;

    public Slice(Metadata meta, ColumnKey begin, ColumnKey end, List<Column> columns)
    {
        super(meta, begin, end);
        assert columns != null;
        this.columns = Collections.unmodifiableList(columns);
    }

    @Override
    public int count()
    {
        return columns.size();
    }

    @Override
    public List<Column> columns()
    {
        return columns;
    }
}
