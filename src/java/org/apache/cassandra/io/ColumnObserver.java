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

package org.apache.cassandra.io;

import java.nio.ByteBuffer;
import java.util.SortedSet;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * An observer of column values for a particular column name. Implementations will receive a call
 * to 'observe' when a value for the given column name is located. Observers should only be shown
 * column values after garbage collection.
 */
public abstract class ColumnObserver implements Comparable<ColumnObserver>
{
    // name of the column we'd like to observe (and the name's type)
    public final ByteBuffer name;
    public final AbstractType type;

    public ColumnObserver(ByteBuffer name, AbstractType type)
    {
        this.name = name;
        this.type = type;
    }

    public final void maybeObserve(ColumnFamily row)
    {
        IColumn col = row.getColumn(name);
        if (col != null && !col.isMarkedForDelete()) observe(col.value());
    }

    abstract protected void observe(ByteBuffer value);

    @Override
    public int compareTo(ColumnObserver that)
    {
        assert this.type == that.type;
        if (this == that)
            return 0;
        return compareToName(that.name);
    }

    public int compareToName(ByteBuffer name)
    {
        return this.type.compare(this.name, name);
    }

    /**
     * Wrapper iterator that observes IColumns passed through it with a set of Observers.
     */
    public static class Iterator extends AbstractIterator<IColumn>
    {
        private final java.util.Iterator<IColumn> src;
        private final PeekingIterator<ColumnObserver> observers;

        private Iterator(java.util.Iterator<IColumn> src, SortedSet<? extends ColumnObserver> observers)
        {
            super();
            this.src = src;
            this.observers = Iterators.peekingIterator(observers.iterator());
        }
        
        public static java.util.Iterator<IColumn> apply(java.util.Iterator<IColumn> src, SortedSet<? extends ColumnObserver> observers)
        {
            if (observers.isEmpty())
                return src;
            return new Iterator(src, observers);
        }

        @Override
        protected IColumn computeNext()
        {
            if (!src.hasNext())
                return endOfData();

            IColumn col = src.next();
            if (col.isMarkedForDelete())
                return col;

            // see if anyone wants to observe the current column
            while (observers.hasNext())
            {
                int c = observers.peek().compareToName(col.name());
                if (c < 0)
                    // haven't reached the next column to observe: continue
                    break;
                else if (c == 0)
                {
                    // found an observed column: pop the observer and observe
                    observers.next().observe(col.value());
                    break;
                }
                else
                    // this observer missed its chance to observe: skip it
                    observers.next();
            }
            return col;
        }
    }
}
