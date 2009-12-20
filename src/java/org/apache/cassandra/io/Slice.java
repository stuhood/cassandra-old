/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io;

import java.util.*;
import java.io.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.Pair;


/**
 * An immutable object representing a Slice read from disk: A Slice is a sorted
 * sequence of columns within a SSTable block that share the same parents, and thus
 * the same Metadata.
 *
 * A Slice contains columns between currentKey, inclusive, and nextKey, exclusive.
 */
public class Slice
{
    public final Metadata meta;
    // the key of the first column: all but the last name will be equal for
    // columns in the slice
    public final ColumnKey currentKey;
    // first key of the next slice on disk (exclusive end to our range)
    public final ColumnKey nextKey;

    /**
     * @param meta Metadata for the parents of this Slice.
     * @param currentKey The key for the first column in the Slice.
     * @param nextKey The key for the first column of the next Slice, or null if
     * there are no more slices in this context.
     */
    Slice(Metadata meta, ColumnKey currentKey, ColumnKey nextKey)
    {
        assert meta != null && currentKey != null;
        this.meta = meta;
        this.currentKey = currentKey;
        this.nextKey = nextKey;
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<Slice ").append(currentKey).append(" ").append(nextKey).append(">");
        return buff.toString();
    }

    /**
     * Metadata shared between columns in a Slice: currently only contains deletion
     * info.
     *
     * Implemented as an immutable singly linked list of Metadata objects from
     * children to parents: to determine if a Column has been deleted, you can
     * iterate from the head of a Metadata list to the tail, comparing deletion info.
     */
    static final class Metadata
    {
        // TODO: document the actual meaning of these fields
        public final long markedForDeleteAt;
        public final int localDeletionTime;

        // next item (our parent) in the list
        public final Metadata parent;
        // our depth: our parent has depth-1, and the root has depth 0
        private final byte depth;

        Metadata()
        {
            this(Long.MIN_VALUE, Integer.MIN_VALUE);
        }

        Metadata(long markedForDeleteAt, int localDeletionTime)
        {
            this(markedForDeleteAt, localDeletionTime, null);
        }

        private Metadata(long markedForDeleteAt, int localDeletionTime, Metadata parent)
        {
            this.markedForDeleteAt = markedForDeleteAt;
            this.localDeletionTime = localDeletionTime;
            this.parent = parent;
            depth = (parent == null) ? 0 : (byte)(1+parent.depth);
        }

        /**
         * Returns an extended list by adding the given metadata as a child of
         * this parent Metadata object.
         */
        public Metadata childWith(long markedForDeleteAt, int localDeletionTime)
        {
            return new Metadata(markedForDeleteAt, localDeletionTime, this);
        }

        /**
         * @return Metadata for the given depth: asserts that the depth exists.
         */
        public Metadata get(int depth)
        {
            assert this.depth >= depth;
            if (this.depth == depth)
                return this;
            return get(depth++);
        }

        /**
         * Serialize this Metadata list.
         */
        public void serialize(DataOutput dos) throws IOException
        {
            // write the length of the list
            dos.writeByte((byte)(depth+1));
            Metadata meta = this;
            while (meta != null)
            {
                dos.writeLong(meta.markedForDeleteAt);
                dos.writeInt(meta.localDeletionTime);
                meta = meta.parent;
            }
        }

        /**
         * Recursively deserialize a Metadata list.
         */
        public static Metadata deserialize(DataInput dis) throws IOException
        {
            byte depth = dis.readByte();
            return deserialize(dis, depth);
        }

        private static Metadata deserialize(DataInput dis, byte depth) throws IOException
        {
            return new Metadata(dis.readLong(), dis.readInt(),
                                (depth == 0 ? null : deserialize(dis, --depth)));
        }
    }
}
