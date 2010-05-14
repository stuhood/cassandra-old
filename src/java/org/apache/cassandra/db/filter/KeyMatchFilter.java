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

class KeyMatchFilter implements IFilter<DecoratedKey>
{
    final DecoratedKey key;

    public KeyMatchFilter(DecoratedKey key)
    {
        assert key != null;
        this.key = key;
    }

    @Override
    public DecoratedKey initial()
    {
        return key;
    }

    @Override
    public MatchResult<DecoratedKey> matchesBetween(DecoratedKey begin, DecoratedKey end)
    {
        if (end.compareTo(key) < 0)
            // positioned before our key: instruct the scanner to seek forward
            return MatchResult.<DecoratedKey>get(false, MatchResult.OP_SEEK, key);
        if (key.compareTo(begin) < 0)
            // positioned after our key: indicate that we are finished
            return MatchResult.NOMATCH_DONE;
        return MatchResult.MATCH_CONT;
    }

    @Override
    public boolean matches(DecoratedKey key)
    {
        return this.key.equals(key);
    }

    @Override
    public String toString()
    {
        return "#<KeyMatchFilter " + key + ">";
    }
}
