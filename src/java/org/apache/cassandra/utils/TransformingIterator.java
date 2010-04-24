package org.apache.cassandra.utils;
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


import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * Transforms values from a source iterator to zero or more values. Usually only useful for dealing with sorted inputs.
 *
 * Subclasses will have their 'transform' method called with successive values until it returns false for a value R, at
 * which point 'transformed' will be called once to retrieve a complete value, and calls to 'transform' will continue
 * with value R.
 */
public abstract class TransformingIterator<T1, T2> extends AbstractIterator<T2> implements Iterator<T2>, PeekingIterator<T2>
{
    protected Iterator<T1> source;
    private PeekingIterator<T1> psource;

    public TransformingIterator(Iterator<T1> source)
    {
        this.source = source;
        this.psource = Iterators.peekingIterator(this.source);
    }

    /**
     * Called to merge the given object from the source with the previous ones.
     * @return True if the value is accepted, or false if a merged value is ready and the given value should be retried.
     */
    protected abstract boolean transform(T1 current);

    /**
     * Called (possibly multiple times) after all values from the source have been successfully accepted by transform.
     * @return True if a call to transformed() will return a merged value.
     */
    protected abstract boolean complete();

    /**
     * @return The object computed by the previous successful calls to transform.
     */
    protected abstract T2 transformed();

    protected T2 computeNext()
    {
        while (psource.hasNext())
        {
            // peek at the next value, without consuming
            T1 value = psource.peek();
            if (transform(value))
            {
                // successfully merged: consume the value from the source
                psource.next();
                continue;
            }
            // a merged value is available
            return transformed();
        }

        // source is out of values: empty our state
        if (complete())
            return transformed();
        return endOfData();
    }
}
