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

import java.io.*;

/**
 * An input stream which bounds reads from the wrapped stream.
 */
public final class BoundedInputStream extends InputStream
{
    private final InputStream in;
    private int available;
    BoundedInputStream(InputStream in, int bound)
    {
        this.in = in;
        available = bound;
    }

    @Override
    public int available() throws IOException
    {
        return Math.min(in.available(), available);
    }
    
    @Override
    public void close() throws IOException
    {
        in.close();
    }

    @Override
    public long skip(long n) throws IOException
    {
        long skip = in.skip(Math.min(available, n));
        available -= skip;
        return skip;
    }

    @Override
    public int read() throws IOException
    {
        if (available == 0)
            return -1;
        int read = in.read();
        if (read != -1)
            available--;
        return read;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int read = in.read(b, off, Math.min(len, available));
        available -= read;
        return read;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }
}
