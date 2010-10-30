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

package org.apache.cassandra.utils;

import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.MapMaker;

/**
 * A pool of interned values of type V. Uses weak references internally to automatically collect values that are no
 * longer used outside the pool.
 */
public class InternPool<K, V>
{
    private final Function<K, K> copy;
    private final Function<K, V> ktov;
    private final Predicate<Void> shouldIntern;
    private final ConcurrentMap<K, V> pool;

    /**
     * @param copy A function to be executed (optionally) on keys before interning: the output should be immutable.
     * @param ktov A function that computes interned values from inputs, or null if keys should be identical to values.
     * @param shouldIntern A predicate to be called before every attempt to intern.
     */
    public InternPool(Function<K, K> copy, Function<K, V> ktov, Predicate<Void> shouldIntern)
    {
        this.copy = copy;
        this.ktov = ktov;
        this.shouldIntern = shouldIntern;
        MapMaker mm = new MapMaker().concurrencyLevel(40).weakValues();
        if (ktov == null)
            // keys and values will be identical, so both must be weak
            mm = mm.weakKeys();
        this.pool = mm.makeMap();
    }

    /**
     * Intended for use with long-lived values.
     * @param input The input key to intern.
     * @param copyFirst True if the key should be copied before interning.
     * @return A (possibly) interned value.
     */
    @SuppressWarnings(value="unchecked")
    public V intern(K input, boolean copyFirst)
    {
        V val = pool.get(input);
        if (val != null)
            return val;
        K key = copyFirst ? copy.apply(input) : input;
        val = ktov == null ? (V)key : ktov.apply(key);
        if (shouldIntern.apply(null)) pool.put(key, val);
        return val;
    }

    /**
     * Intended for use with short-lived values.
     * @return The interned value for the input, or null if it does not exist: will not modify the pool.
     */
    public V getIfInterned(K input)
    {
        return pool.get(input);
    }
}
