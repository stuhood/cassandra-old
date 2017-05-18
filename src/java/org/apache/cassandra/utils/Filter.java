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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.ICompactSerializer;

public abstract class Filter
{
    int hashCount;

    private static MurmurHash hasher = new MurmurHash();

    int getHashCount()
    {
        return hashCount;
    }

    public int[] getHashBuckets(ByteBuffer key)
    {
        return Filter.getHashBuckets(key, hashCount, buckets());
    }

    abstract int buckets();

    public abstract void add(ByteBuffer key);

    public abstract boolean isPresent(ByteBuffer key);

    // for testing
    abstract int emptyBuckets();

    ICompactSerializer<Filter> getSerializer()
    {
        Method method = null;
        try
        {
            method = getClass().getMethod("serializer");
            return (ICompactSerializer<Filter>) method.invoke(null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    static int[] getHashBuckets(ByteBuffer b, int hashCount, int max)
    {
        int[] result = new int[hashCount];
        int hash1 = hasher.hash(b.array(), b.position()+b.arrayOffset(), b.remaining(), 0);
        int hash2 = hasher.hash(b.array(), b.position()+b.arrayOffset(), b.remaining(), hash1);
        for (int i = 0; i < hashCount; i++)
        {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }
}
