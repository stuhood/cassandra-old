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

package org.apache.cassandra.io.sstable.bitidx;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.io.sstable.bitidx.avro.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Joins iterators of OpenSegments together using a boolean operator. Asserts that an equal number
 * of segments are received from each source.
 */
final class JoiningIterator extends ReducingIterator<OpenSegment,OpenSegment> implements CloseableIterator<OpenSegment>
{
    static final Logger logger = LoggerFactory.getLogger(JoiningIterator.class);

    private final Operator op;
    private final Collection<CloseableIterator<OpenSegment>> sources;
    private final int numSources;

    private int numReduced;
    private OpenSegment segment;

    public JoiningIterator(Operator op, Collection<CloseableIterator<OpenSegment>> sources)
    {
        super(getCollatingIterator(sources));
        this.op = op;
        this.sources = sources;
        this.numSources = sources.size();
        this.numReduced = 0;
        // TODO: pool for OpenSegments
        this.segment = new OpenSegment(BitmapIndex.SEGMENT_SIZE_BITS);
    }

    private static CollatingIterator getCollatingIterator(Collection<CloseableIterator<OpenSegment>> sources)
    {
        CollatingIterator citer = FBUtilities.<OpenSegment>getCollatingIterator();
        for (CloseableIterator<OpenSegment> source : sources)
            citer.addIterator(source);
        return citer;
    }

    @Override
    public void reduce(OpenSegment inc)
    {
        if (numReduced++ == 0)
        {
            // first segment for a new rowid range
            segment.copy(inc);
            return;
        }
        assert segment.numrows == inc.numrows : "Can only join equal length segments";
        op.apply(segment, inc);
    }

    @Override
    protected OpenSegment getReduced()
    {
        assert numReduced == numSources : "Must receive segments from each source.";
        numReduced = 0;
        return segment;
    }

    public void close() throws IOException
    {
        for (CloseableIterator<OpenSegment> source : sources)
            source.close();
    }

    static abstract class Operator
    {
        static final Operator AND = new Operator()
        {
            public void apply(OpenSegment target, OpenSegment source)
            {
                target.bitset.intersect(source.bitset);
            }
        };

        static final Operator OR = new Operator()
        {
            public void apply(OpenSegment target, OpenSegment source)
            {
                target.bitset.union(source.bitset);
            }
        };

        /**
         * Applies this boolean operator to the inputs, putting the results in target, and leaving source unchanged.
         */
        public abstract void apply(OpenSegment target, OpenSegment source);
    }
}
