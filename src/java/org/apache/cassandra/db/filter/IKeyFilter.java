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

package org.apache.cassandra.db.filter;

import org.apache.cassandra.db.*;

/**
 * Given an implementation-specific description of what keys to look for, provides methods
 * to extract the desired columns from a set of Slices. QueryFilter takes care of putting a
 * KeyFilter together with NameFilters, based on the querypath that it knows (but that
 * IKeyFilter implementations are oblivious to).
 */
interface IKeyFilter
{
    /**
     * @return True if this filter matches the given key.
     */
    public abstract boolean matchesKey(DecoratedKey key);
}
