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

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.StringTokenizer;

import com.google.common.base.Objects;

import org.apache.cassandra.utils.Pair;

/**
 * SSTables are made up of multiple components in separate files. Components are
 * identified by a type and an id, but required unique components (such as the Data
 * and Index files) may have implicit ids assigned to them.
 */
public class Component
{
    public static final EnumSet<Type> TYPES = EnumSet.allOf(Type.class);
    public static final EnumSet<Type> INDEX_TYPES = EnumSet.of(Type.FILTER, Type.PRIMARY_INDEX, Type.BITMAP_INDEX);

    public static enum Type
    {
        // the base data for an sstable: the remaining components can be regenerated
        // based on the data component
        DATA("Data.db"),
        // index of the row keys with pointers to their positions in the data file
        PRIMARY_INDEX("Index.db"),
        // serialized bloom filter for the row keys in the sstable
        FILTER("Filter.db"),
        // 0-length file that is created when an sstable is ready to be deleted
        COMPACTED_MARKER("Compacted"),
        // statistical metadata about the content of the sstable
        STATS("Statistics.db"),
        // a bitmap secondary index: many of these may exist per sstable
        BITMAP_INDEX("Bitidx.db"),
        // a temporary component used during writing and removed before close
        SCRATCH("Scratch.db");

        final String repr;
        Type(String repr)
        {
            this.repr = repr;
        }
        
        static Type fromRepresentation(String repr)
        {
            for (Type type : TYPES)
                if (repr.equals(type.repr))
                    return type;
            throw new RuntimeException("Invalid SSTable component: '" + repr + "'");
        }
    }

    // singleton components for types that don't need ids
    public final static Component DATA = new Component(Type.DATA, -1);
    public final static Component PRIMARY_INDEX = new Component(Type.PRIMARY_INDEX, -1);
    public final static Component FILTER = new Component(Type.FILTER, -1);
    public final static Component COMPACTED_MARKER = new Component(Type.COMPACTED_MARKER, -1);
    public final static Component STATS = new Component(Type.STATS, -1);

    public final Type type;
    public final int id;
    public final int hashCode;

    /** Private: use get. */
    private Component(Type type, int id)
    {
        this.type = type;
        this.id = id;
        this.hashCode = Objects.hashCode(type, id);
    }

    public static Component get(Type type)
    {
        return get(type, -1);
    }

    /** @return The Component for the given Type and id (often a singleton) */
    public static Component get(Type type, int id)
    {
        switch(type)
        {
            case DATA:              return Component.DATA;
            case PRIMARY_INDEX:     return Component.PRIMARY_INDEX;
            case FILTER:            return Component.FILTER;
            case COMPACTED_MARKER:  return Component.COMPACTED_MARKER;
            case STATS:             return Component.STATS;
            case BITMAP_INDEX:      // fall through
            case SCRATCH:           return new Component(type, id);
            default:                throw new IllegalStateException();
        }
    }

    /**
     * @return The unique (within an sstable) name for this component.
     */
    public String name()
    {
        switch(type)
        {
            case DATA:
            case PRIMARY_INDEX:
            case FILTER:
            case COMPACTED_MARKER:
            case STATS:
                return type.repr;
            case BITMAP_INDEX:
            case SCRATCH:
                return String.format("%d-%s", id, type.repr);
        }
        throw new IllegalStateException();
    }

    /**
     * Filename of the form "<ksname>/<cfname>-[tmp-][<version>-]<gen>-<component>",
     * where <component> is of the form "[<id>-]<component>".
     * @return A Descriptor for the SSTable, and a Component for this particular file.
     * TODO move descriptor into Component field
     */
    public static Pair<Descriptor,Component> fromFilename(File directory, String name)
    {
        Pair<Descriptor,StringTokenizer> path = Descriptor.fromFilename(directory, name);

        // parse the component suffix
        int id = -1;
        String repr = path.right.nextToken();
        if (path.right.hasMoreTokens())
        {
            id = Integer.parseInt(repr);
            repr = path.right.nextToken();
        }
        Type type = Type.fromRepresentation(repr);
        // build (or retrieve singleton for) the component object
        Component component = get(type, id);
        return new Pair<Descriptor,Component>(path.left, component);
    }

    @Override
    public String toString()
    {
        return this.name();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Component))
            return false;
        Component that = (Component)o;
        return this.type == that.type && this.id == that.id;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    /** Generates unique component ids. */
    public static class IdGenerator
    {
        private final AtomicInteger next = new AtomicInteger(1);
        public int getNextId()
        {
            return next.getAndIncrement();
        }
    }
}
