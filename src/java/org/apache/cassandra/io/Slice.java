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
 */
public class Slice
{
    public final Metadata parentMeta;
    // the key of the first column: all but the last name will be equal for columns in the slice
    public final ColumnKey firstKey;
    // immutable sequence of immutable columns
    public final Iterable<Column> columns;

    /**
     * @param parentMeta Metadata for the parents of this Slice.
     * @param firstKey The key for the first column in the Slice.
     * @param columns Once ownership of the column list is passed to a Slice,
     *        it should not be modified.
     */
    Slice(Metadata parentMeta, ColumnKey firstKey, List<Column> columns)
    {
        this.parentMeta = parentMeta;
        this.firstKey = firstKey;
        this.columns = Collections.unmodifiableList(columns);
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<Slice ").append(firstKey).append(" ").append(columns).append(">");
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
        // TODO: document the meaning of these fields
        public final long markedForDeleteAt;
        public final int localDeletionTime;

        // next item (our parent) in the list
        public final Metadata parent;

        Metadata(long markedForDeleteAt, int localDeletionTime)
        {
            this(markedForDeleteAt, localDeletionTime, null);
        }

        Metadata(long markedForDeleteAt, int localDeletionTime, Metadata parent)
        {
            this.markedForDeleteAt = markedForDeleteAt;
            this.localDeletionTime = localDeletionTime;
            this.parent = parent;
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
         * Serialize a Metadata list, which may be null to indicate an empty list.
         */
        public static void serialize(Metadata meta, DataOutput dos) throws IOException
        {
            while (meta != null)
            {
                dos.writeBoolean(true);
                dos.writeLong(meta.markedForDeleteAt);
                dos.writeInt(meta.localDeletionTime);
                meta = meta.parent;
            }
            dos.writeBoolean(false);
        }

        /**
         * Deserialize a Metadata list.
         */
        public static Metadata deserialize(DataInput dis) throws IOException
        {
            if (!dis.readBoolean())
                return null;
            // recursively
            return new Metadata(dis.readLong(), dis.readInt(), deserialize(dis));
        }
    }
}
