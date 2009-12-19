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

import java.io.IOException;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.log4j.Logger;
import com.google.common.collect.AbstractIterator;

public class SSTableScanner extends AbstractIterator<IteratingRow> implements Closeable
{
    private static Logger logger = Logger.getLogger(SSTableScanner.class);

    private IteratingRow row;
    private BufferedRandomAccessFile file;
    private SSTableReader sstable;
    private Iterator<IteratingRow> iterator;

    SSTableScanner(SSTableReader sstable, long blockPosition) throws IOException
    {
        // TODO this is used for both compactions and key ranges.  the buffer sizes we want
        // to use for these ops are very different.  here we are leaning towards the key-range
        // use case since that is more common.  What we really want is to split those
        // two uses of this class up.
        this.file = new BufferedRandomAccessFile(sstable.getFilename(), "r", 256 * 1024);
        this.sstable = sstable;
    }

    public void close() throws IOException
    {
        file.close();
    }

    /**
     * Seeks to the first slice for the given key.
     */
    public void seekTo(DecoratedKey seekKey)
    {
        seekTo(new ColumnKey(seekKey, new byte[0][]));
    }

    /**
     * Seeks to the first slice for the given key.
     * FIXME: optimize forward seeks within the same block by not reopening the block
     */
    public void seekTo(ColumnKey seekKey)
    {
        try
        {
            long position = sstable.getBlockPosition(seekKey);
            if (position < 0)
                return;
            file.seek(position);
            row = null;
        }
        catch (IOException e)
        {
            throw new RuntimeException("corrupt sstable", e);
        }
    }

    public IteratingRow computeNext()
    {
        if (iterator == null)
            iterator = new KeyScanningIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private class KeyScanningIterator implements Iterator<IteratingRow>
    {
        public boolean hasNext()
        {
            try
            {
                return (row == null && !file.isEOF()) || row.getEndPosition() < file.length();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public IteratingRow next()
        {
            try
            {
                if (row != null)
                    row.skipRemaining();
                assert !file.isEOF();
                return row = new IteratingRow(file, sstable);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
