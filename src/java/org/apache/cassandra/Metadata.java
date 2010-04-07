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

package org.apache.cassandra;

import java.io.*;

/**
 * Metadata shared between columns in a Slice: currently only contains deletion
 * info.
 *
 * Implemented as an immutable singly linked list of Metadata objects from
 * children to parents.
 */
public final class Metadata
{
    public final long markedForDeleteAt;
    public final int localDeletionTime;

    // next item (our parent) in the list
    public final Metadata parent;
    // our depth: our parent has depth-1, and the root has depth 0
    private final byte depth;

    public Metadata()
    {
        this(Long.MIN_VALUE, Integer.MIN_VALUE);
    }

    public Metadata(long markedForDeleteAt, int localDeletionTime)
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
     * Recursively resolves two Metadata lists of equal depth.
     *
     * @return A new 'winning' list.
     */
    public static Metadata resolve(Metadata lhs, Metadata rhs)
    {
        Metadata parent = lhs.parent != null ?
            resolve(lhs.parent, rhs.parent) : null;
        long markedForDeleteAt = Math.max(lhs.markedForDeleteAt,
                                          rhs.markedForDeleteAt);
        int localDeletionTime =  Math.max(lhs.localDeletionTime,
                                          rhs.localDeletionTime);
        return new Metadata(markedForDeleteAt, localDeletionTime, parent);
    }

    /**
     * @return The max markedForDeleteAt value contained in this Metadata list.
     */
    public long getMarkedForDeleteAt()
    {
        return parent != null ?
            Math.max(parent.getMarkedForDeleteAt(), markedForDeleteAt) :
            markedForDeleteAt;
    }

    /**
     * @return The max localDeletionTime value contained in this Metadata list.
     */
    public int getLocalDeletionTime()
    {
        return parent != null ?
            Math.max(parent.getLocalDeletionTime(), localDeletionTime) :
            localDeletionTime;
    }

    /**
     * @return Metadata for the given depth: asserts that the depth exists.
     */
    public Metadata get(int depth)
    {
        if (this.depth == depth)
            return this;
        assert parent != null :
            "Incorrect metadata depth " + depth + " for column family.";
        return parent.get(depth);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Metadata))
            return false;

        Metadata that = (Metadata)o;
        if (this.markedForDeleteAt != that.markedForDeleteAt)
            return false;
        if (this.localDeletionTime != that.localDeletionTime)
            return false;
        if ((this.parent == null) != (that.parent == null))
            return false;
        return this.parent == null ? true : this.parent.equals(that.parent);
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

    @Override
    public String toString()
    {
        return markedForDeleteAt + "$" + localDeletionTime +
            (parent == null ? "." : "," + parent.toString());
    }

    /**
     * Recursively deserialize a Metadata list.
     */
    public static Metadata deserialize(DataInput dis) throws IOException
    {
        byte remaining = dis.readByte();
        return deserialize(dis, remaining);
    }

    private static Metadata deserialize(DataInput dis, byte remaining) throws IOException
    {
        return new Metadata(dis.readLong(), dis.readInt(),
                            --remaining == 0 ? null : deserialize(dis, remaining));
    }
}
