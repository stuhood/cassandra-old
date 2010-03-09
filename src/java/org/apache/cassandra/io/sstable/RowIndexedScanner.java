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

package org.apache.cassandra.io.sstable;

import java.io.*;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.Slice;
import org.apache.cassandra.io.SliceBuffer;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;

import com.google.common.collect.AbstractIterator;

import org.apache.log4j.Logger;

/**
 * In order to provide lazy behaviour, we implement hasNext and next directly rather than
 * via AbstractIterator (which eagerly calculates next).
 */
public class RowIndexedScanner implements SSTableScanner
{
    private static final Logger logger = Logger.getLogger(RowIndexedScanner.class);

    private final RowIndexedReader reader;
    private final long length;
    private final int bufferSize;
    private BufferedRandomAccessFile file;
    private final ColumnFamily emptycf; // always empty: just a metadata holder

    /**
     * State related to the current row.
     */
    protected Slice.Metadata rowmeta;
    protected DecoratedKey rowkey;
    private long rowoffset;            // immediately before the key
    private long rowdataoffset;        // after key and length
    private long rowcolsoffset;        // at first column
    private int rowlength;
    private List<IndexHelper.IndexInfo> rowindex;
    private int chunkpos; // position of current chunk in the rowindex

    /**
     * @param reader SSTable to scan.
     * @param bufferSize Buffer size for the file backing the scanner (if supported).
     */
    RowIndexedScanner(RowIndexedReader reader, int bufferSize) throws IOException
    {
        super();
        this.reader = reader;
        this.length = reader.length();
        this.bufferSize = bufferSize;
        emptycf = reader.makeColumnFamily();
    }

    @Override
    public int columnDepth()
    {
        return reader().getColumnDepth();
    }

    @Override
    public ColumnKey.Comparator comparator()
    {
        return reader().getComparator();
    }

    @Override
    public RowIndexedReader reader()
    {
        return reader;
    }

    /**
     * Moves to the row at the given offset, lazily (re)opening the file if necessary.
     */
    protected void repositionRow(long offset) throws IOException
    {
        if (file == null)
            file = new BufferedRandomAccessFile(reader.getFilename(), "r", bufferSize);
        file.seek(offset);

        // update mutable state for the row
        rowoffset = offset;
        rowkey = reader.getPartitioner().convertFromDiskFormat(file.readUTF());
        rowlength = file.readInt();
        rowdataoffset = file.getFilePointer();
        // TODO: selectively load bloom filter?
        IndexHelper.skipBloomFilter(file);
        rowindex = IndexHelper.deserializeIndex(file);
        // row metadata
        ColumnFamily.serializer().deserializeFromSSTableNoColumns(emptycf, file);
        rowmeta = new Slice.Metadata(emptycf.getMarkedForDeleteAt(),
                                     emptycf.getLocalDeletionTime());

        // current slice within the cf
        chunkpos = -1;
        file.readInt(); // column count
        rowcolsoffset = file.getFilePointer();
    }

    /**
     * Sees whether more index chunks are available.
     */
    protected boolean canIncrementChunk()
    {
        if (file == null)
            return false;
        if (chunkpos < rowindex.size() - 1)
            return true;
        if (rowdataoffset + rowlength < length)
            return true;
        return false;
    }

    /**
     * Moves to the next index chunk: assumes a row is open.
     */
    protected boolean incrementChunk() throws IOException
    {
        if (chunkpos < rowindex.size() - 1)
        {
            // next index chunk in the current row
            chunkpos++;
            return true;
        }
        if (rowdataoffset + rowlength < length)
        {
            // next row in the file
            repositionRow(rowdataoffset + rowlength);
            chunkpos++;
            return true;
        }
        return false;
    }

    /**
     * Un-positions a Scanner, so that it isn't pointed anywhere.
     */
    protected void clearPosition()
    {
        rowmeta = null;
    }

    @Override
    public void close() throws IOException
    {
        if (file != null)
            file.close();
    }

    @Override
    public long getBytesRemaining()
    {
        if (file == null)
            return length;
        return length - file.getFilePointer();
    }

    @Override
    public boolean first() throws IOException
    {
        repositionRow(0);
        return true;
    }

    @Override
    public boolean seekNear(DecoratedKey seekKey) throws IOException
    {
        long position = reader.getNearestPosition(seekKey);
        if (position < 0)
        {
            clearPosition();
            return false;
        }
        repositionRow(position);
        return true;
    }

    @Override
    public boolean seekNear(ColumnKey seekKey) throws IOException
    {
        if (!seekNear(seekKey.dk))
            return false;
        // positioned at row: seek within rowindex
        chunkpos = IndexHelper.indexFor(seekKey.name(1), rowindex,
                                        reader.getColumnComparator(), false);
        if (chunkpos >= rowindex.size())
            return false;
        // position before matching chunk
        chunkpos--;
        return true;
    }

    @Override
    public boolean seekTo(DecoratedKey seekKey) throws IOException
    {
        SSTable.PositionSize position = reader.getPosition(seekKey);
        if (position == null)
        {
            clearPosition();
            return false;
        }
        repositionRow(position.position);
        return true;
    }

    @Override
    public boolean seekTo(ColumnKey seekKey) throws IOException
    {
        if (!seekTo(seekKey.dk))
            return false;
        // positioned at row: seek within rowindex
        chunkpos = IndexHelper.indexFor(seekKey.name(1), rowindex,
                                        reader.getColumnComparator(), false);
        if (chunkpos >= rowindex.size())
            return false;
        // position before matching chunk
        chunkpos--;
        return true;
    }

    @Override
    public boolean hasNext()
    {
        if (file == null)
            return false;
        return canIncrementChunk();
    }

    @Override
    public SliceBuffer next()
    {
        assert file != null : "A Scanner must be positioned before use.";
        try
        {
            if (incrementChunk())
                return getChunkSlice();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        throw new NoSuchElementException();
    }

    @Override
    public final void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The slice for the current chunk of the index.
     */
    private SliceBuffer getChunkSlice()
    {
        assert rowmeta != null;
        assert chunkpos < rowindex.size();
        
        // slices are inclusive,exclusive, but row indexes are inclusive,inclusive
        byte[] start = (chunkpos == 0) ?
            // first column of the chunk (rounded down to NAME_BEGIN for first chunk)
            ColumnKey.NAME_BEGIN : rowindex.get(chunkpos).firstName;
        byte[] end = (chunkpos < rowindex.size()-1) ?
            // first column of next chunk (rounded up to NAME_END for last chunk)
            rowindex.get(chunkpos+1).lastName : ColumnKey.NAME_END;
        return new SliceBuffer(rowmeta, new ColumnKey(rowkey, start), new ColumnKey(rowkey, end), (List<Column>)getRawColumns());
    }

    /**
     * @return Un-typed columns from disk for the current chunk of the index.
     */
    protected List getRawColumns() throws IOException
    {
        if (rowmeta == null)
            return null;
        
        // read sequential columns from the chunk
        ArrayList<IColumn> columns = new ArrayList<IColumn>();
        file.seek(rowcolsoffset + rowindex.get(chunkpos).offset);
        long chunkend = file.getFilePointer() + rowindex.get(chunkpos).width;
        while (file.getFilePointer() < chunkend)
            columns.add(emptycf.getColumnSerializer().deserialize(file));
        return columns;
    }
}
