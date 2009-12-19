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
 * A FilterInputStream that prevents access to a wrapped stream beyond a certain point.
 */
public class BoundedInputStream extends FilterInputStream
{
    // the current and maximum offsets within the wrapped stream
    private int cur;
    private int max;

    /**
     * @param in The input stream to wrap. 
     * @param length The length from the current position that this stream will read.
     */
    BoundedInputStream(InputStream in, int length)
    {
        super(in);
        cur = 0;
        max = length;
    }

    @Override
    public int available() throws IOException
    {
        return Math.min(max-cur, in.available());
    }

    /**
     * Does nothing.
     */
    @Override
    public void close()
    {
        return;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public int read() throws IOException
    {
        if (cur >= max)
            return -1;
        int read = in.read();
        if (read != -1)
            cur++;
        return read;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        int read = in.read(b, 0, Math.min(max-cur, b.length));
        if (read > 0)
            cur += read;
        assert cur <= max;
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int read = in.read(b, off, Math.min(max-cur, len));
        if (read > 0)
            cur += read;
        assert cur <= max;
        return read;
    }

    @Override
    public long skip(long n) throws IOException
    {
        long skipped = in.skip(Math.min(max-cur, n));
        if (skipped > 0)
            cur += skipped;
        assert cur <= max;
        return skipped;
    }
}
