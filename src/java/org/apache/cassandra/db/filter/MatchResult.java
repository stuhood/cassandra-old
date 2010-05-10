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

/**
 * Describes the result of matching a Slice against a filter, which allows a filter to suggest an order of operations
 * to a Scanner for a particular query.
 */
final class MatchResult<T>
{
    // the filter is interested in nearby values, so the scanner should continue forward
    public static final int OP_CONT = 0;
    // the next match is expected to be a reasonable distance away: 'seekkey' will be set to the next name the filter
    // is interested in
    public static final int OP_SEEK = 1;
    // the filter is satisfied that no more matches exist
    public static final int OP_DONE = 2;

    // singletons for results which don't need keys
    public static final MatchResult NOMATCH_CONT = new MatchResult(false, OP_CONT, null);
    public static final MatchResult NOMATCH_DONE = new MatchResult(false, OP_DONE, null);
    public static final MatchResult MATCH_CONT = new MatchResult(true, OP_CONT, null);
    public static final MatchResult MATCH_DONE = new MatchResult(true, OP_DONE, null);

    // true if the match was successful
    public final boolean matched;
    // code describing where to look for the next possible match
    public final int hint;
    // location of next possible match (or null)
    public final T seekkey;

    /**
     * Use the factory function to minimize object creation.
     */
    private MatchResult(boolean matched, int hint, T seekkey)
    {
        this.matched = matched;
        this.hint = hint;
        this.seekkey = seekkey;
    }

    public static <H> MatchResult<H> get(boolean matched, int hint, H seekkey)
    {
        switch (hint)
        {
            case OP_CONT:
                return matched ? MATCH_CONT : NOMATCH_CONT;
            case OP_SEEK:
            {
                assert seekkey != null : "The SEEK operation must provide a key.";
                return new MatchResult<H>(matched, hint, seekkey);
            }
            case OP_DONE:
                return matched ? MATCH_DONE : NOMATCH_DONE;
            default:
                throw new RuntimeException("Unsupported value for hint: use MatchResult.OP_*");
        }
    }

    @Override
    public String toString()
    {
        return "#<MatchResult " + matched + ", " + hint + ", " + seekkey + ">";
    }
}
