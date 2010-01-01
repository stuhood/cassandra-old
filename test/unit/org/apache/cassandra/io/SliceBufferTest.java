/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io;

import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.service.StorageService;

public class SliceBufferTest
{
    public static final ColumnKey.Comparator COMPARATOR =
        ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME);

    public static final Column LCOL = new Column("a".getBytes(), "a".getBytes(), 1);
    public static final Column LCOL_TOMB = new Column(LCOL.name(), LCOL.value(), 2, true);
    public static final Column MCOL = new Column("m".getBytes(), "m".getBytes(), 1);
    public static final Column MCOL_TOMB = new Column(MCOL.name(), MCOL.value(), 2, true);
    public static final Column RCOL = new Column("y".getBytes(), "y".getBytes(), 1);

    DecoratedKey dk(String str)
    {
        return StorageService.getPartitioner().decorateKey(str);
    }

    /**
     * Merges two input SliceBuffers, and compares to an ordered list of SliceBuffers.
     */
    public void mergedEquals(SliceBuffer one, SliceBuffer two, SliceBuffer... expectarr)
    {
        exactlyEquals(SliceBuffer.merge(COMPARATOR, one, two), expectarr);
        exactlyEquals(SliceBuffer.merge(COMPARATOR, two, one), expectarr);
    }

    public void exactlyEquals(List<SliceBuffer> actuals, SliceBuffer... expectarr)
    {
        List<SliceBuffer> expecteds = Arrays.asList(expectarr);

        assertEquals("Wrong number of slices", expecteds.size(), actuals.size());
        for (int s = 0; s < expecteds.size(); s++)
        {
            SliceBuffer expected = expecteds.get(s);
            SliceBuffer actual = actuals.get(s);

            assertEquals("Wrong metadata for slice " + s, expected.meta, actual.meta);
            assertEquals("Wrong column count for slice " + s,
                         expected.realized().size(), actual.realized().size());
            assert 0 == COMPARATOR.compare(expected.key, actual.key) :
                "Wrong start key for slice " + s;
            assert 0 == COMPARATOR.compare(expected.end, actual.end) :
                "Wrong end key for slice " + s;
            for (int i = 0; i < expected.realized().size(); i++)
            {
                Column ec = expected.realized().get(i);
                Column ac = actual.realized().get(i);

                assert 0 == COMPARATOR.compareAt(ec.name(), ac.name(),
                                                 COMPARATOR.columnDepth());
                assert Arrays.equals(ec.value(), ac.value());
                assertEquals(ec.isMarkedForDelete(), ac.isMarkedForDelete());
                assertEquals(ec.timestamp(), ac.timestamp());
            }
        }
    }

    /**
     * Buffers with same start and end keys, so no differences to the left or right of
     * the overlap.
     */
    @Test
    public void testMergeOnlyOverlap() throws Exception
    {
        Slice.Metadata min = new Slice.Metadata();
        Slice.Metadata win = new Slice.Metadata(1,1);
        ColumnKey left = new ColumnKey(dk("a"), ColumnKey.NAME_BEGIN);
        ColumnKey right = new ColumnKey(dk("a"), ColumnKey.NAME_END);

        SliceBuffer one = new SliceBuffer(min, left, right, LCOL, RCOL);
        SliceBuffer two = new SliceBuffer(win, left, right, LCOL_TOMB, MCOL);

        // one output slice
        mergedEquals(one, two,
                      new SliceBuffer(win, left, right, LCOL_TOMB, MCOL, RCOL));
    }

    /**
     * One buffer's range completely containing the other.
     */
    @Test
    public void testMergeContaining() throws Exception
    {
        SliceBuffer outer = new SliceBuffer(new Slice.Metadata(),
                                            new ColumnKey(dk("a"), ColumnKey.NAME_BEGIN),
                                            new ColumnKey(dk("a"), ColumnKey.NAME_END),
                                            LCOL, MCOL, RCOL);
        SliceBuffer inner = new SliceBuffer(new Slice.Metadata(1,1),
                                            new ColumnKey(dk("a"), "b".getBytes()),
                                            new ColumnKey(dk("a"), "s".getBytes()),
                                            MCOL_TOMB);

        // three output slices
        mergedEquals(outer, inner,
                      new SliceBuffer(outer.meta, outer.key, inner.key, LCOL),
                      new SliceBuffer(inner.meta, inner.key, inner.end, MCOL_TOMB),
                      new SliceBuffer(outer.meta, inner.end, outer.end, RCOL));
    }

    /**
     * Inputs have equal start keys.
     */
    @Test
    public void testMergeEqualStart() throws Exception
    {
        SliceBuffer outer = new SliceBuffer(new Slice.Metadata(),
                                            new ColumnKey(dk("a"), ColumnKey.NAME_BEGIN),
                                            new ColumnKey(dk("a"), ColumnKey.NAME_END),
                                            LCOL_TOMB, MCOL, RCOL);
        SliceBuffer inner = new SliceBuffer(new Slice.Metadata(1,1),
                                            new ColumnKey(dk("a"), ColumnKey.NAME_BEGIN),
                                            new ColumnKey(dk("a"), "s".getBytes()),
                                            LCOL, MCOL_TOMB);

        // three output slices
        mergedEquals(outer, inner,
                      new SliceBuffer(inner.meta, inner.key, inner.end,
                                      LCOL_TOMB, MCOL_TOMB),
                      new SliceBuffer(outer.meta, inner.end, outer.end, RCOL));
    }
}
