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

package org.apache.cassandra.utils;

import java.io.IOError;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Predicate;
import com.google.common.collect.ForwardingIterator;

/**
 * Filters a CloseableIterator (preserving the interface) to remove values that don't match the predicate.
 */
public class FilterIterator<T> extends ForwardingIterator<T> implements CloseableIterator<T>
{
    private final CloseableIterator<T> source;
    private final Predicate predicate;

    private T nextValue;
    private boolean hasNextValue;

    public FilterIterator(CloseableIterator<T> source, Predicate predicate)
    {
        this.source = source;
        this.predicate = predicate;
    }

    public Iterator<T> delegate()
    {
        return source;
    }

    @Override
    public boolean hasNext()
    {
        if (hasNextValue) return true;

        while (source.hasNext())
        {
            // pop and filter
            nextValue = source.next();
            if (predicate.apply(nextValue))
            {
                hasNextValue = true;
                return true;
            }
        }
        return false;
    }

    @Override
    public T next()
    {
        if (!hasNext()) throw new NoSuchElementException();
        hasNextValue = false;
        return nextValue;
    }

    public void close() throws IOError
    {
        source.close();
    }
}
