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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;

/**
 * Filters keys that match a given AbstractBounds, which must not wrap. To support
 * wrapping bounds, use an And*Filter of two unwrapped bounds.
 */
class KeyRangeFilter implements IKeyFilter
{
    final AbstractBounds bounds;

    public KeyRangeFilter(AbstractBounds bounds)
    {
        assert bounds.unwrap().size() == 1 : "Does not support wrapping bounds!";
        this.bounds = bounds;
    }

    /**
     * TODO: Since Tokens may describe multiple Keys, this filter can not be specific to one Key. See #1034.
     *
     * @return True if this filter matches the given key.
     */
    @Override
    public boolean matchesKey(DecoratedKey key)
    {
        return bounds.contains(key.token);
    }

    @Override
    public String toString()
    {
        return "#<KeyRangeFilter " + bounds + ">";
    }
}
