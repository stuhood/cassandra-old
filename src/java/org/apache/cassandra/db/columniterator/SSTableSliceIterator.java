package org.apache.cassandra.db.columniterator;
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


import java.io.IOError;
import java.io.IOException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

import org.apache.cassandra.utils.FBUtilities;

/**
 *  A Column Iterator over SSTable
 */
public class SSTableSliceIterator implements IColumnIterator
{
    private final FileDataInput fileToClose;
    private IColumnIterator reader;
    private DecoratedKey key;

    /** Private: use create() to conditionally create an iterator. */
    private SSTableSliceIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, byte[] startColumn, byte[] finishColumn, boolean reversed)
    {
        this.key = key;
        this.fileToClose = file;
        reader = createReader(sstable.metadata, file, startColumn, finishColumn, reversed);
    }

    /**
     * An iterator for a slice within an SSTable
     *
     * @param metadata Metadata for the CFS we are reading from
     * @param file Optional parameter that input is read from.  If null is passed, this class creates an appropriate one automatically.
     * If this class creates, it will close the underlying file when #close() is called.
     * If a caller passes a non-null argument, this class will NOT close the underlying file when the iterator is closed (i.e. the caller is responsible for closing the file)
     * In all cases the caller should explicitly #close() this iterator.
     * @param key The key the requested slice resides under
     * @param startColumn The start of the slice
     * @param finishColumn The end of the slice
     * @param reversed Results are returned in reverse order iff reversed is true.
     */
    public SSTableSliceIterator(CFMetaData metadata, FileDataInput file, DecoratedKey key, byte[] startColumn, byte[] finishColumn, boolean reversed)
    {
        this.key = key;
        fileToClose = null;
        reader = createReader(metadata, file, startColumn, finishColumn, reversed);
    }

    /** @return An SSTableSliceIterator, or null if the sstable does not contain data for the key. */
    public static SSTableSliceIterator create(SSTableReader sstable, DecoratedKey key, byte[] startColumn, byte[] finishColumn, boolean reversed) throws IOError
    {
        try
        {
            FileDataInput file = sstable.getFileDataInput(key, DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
            if (file == null)
                // key does not exist in sstable
                return null;
            SSTableReader.readAssertedKey(sstable, file, key);
            SSTableReader.readRowSize(file, sstable.descriptor);
            return new SSTableSliceIterator(sstable, file, key, startColumn, finishColumn, reversed);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private static IColumnIterator createReader(CFMetaData metadata, FileDataInput file, byte[] startColumn, byte[] finishColumn, boolean reversed)
    {
        return startColumn.length == 0 && !reversed
                 ? new SimpleSliceReader(metadata, file, finishColumn)
                 : new IndexedSliceReader(metadata, file, startColumn, finishColumn, reversed);
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily() throws IOException
    {
        return reader.getColumnFamily();
    }

    public boolean hasNext()
    {
        return reader.hasNext();
    }

    public IColumn next()
    {
        return reader.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        if (fileToClose != null)
            fileToClose.close();
    }

}
