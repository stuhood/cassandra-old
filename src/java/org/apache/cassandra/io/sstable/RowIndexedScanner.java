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
import org.apache.cassandra.io.IteratingRow;
import org.apache.cassandra.io.Slice;
import org.apache.cassandra.io.SliceBuffer;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;

import org.apache.log4j.Logger;

public class RowIndexedScanner extends SSTableScanner
{
    private static final Logger logger = Logger.getLogger(RowIndexedScanner.class);

    private final RowIndexedReader reader;
    private final int bufferSize;
    private BufferedRandomAccessFile file;
    private final ColumnFamily emptycf; // always empty: just a metadata holder

    /**
     * State related to the current row.
     */
    protected Slice.Metadata rowmeta;
    protected DecoratedKey rowkey;
    private IteratingRow row;
    private long rowoffset;            // immediately before the key
    private long rowdataoffset;        // after key and length
    private long rowcolsoffset;        // at first column
    private int rowlength;
    private List<IndexHelper.IndexInfo> rowindex;

    // for standard cfs, the current slice is the current index chunk
    private int chunkpos; // position of current chunk in the rowindex

    /**
     * @param reader SSTable to scan.
     * @param bufferSize Buffer size for the file backing the scanner (if supported).
     */
    RowIndexedScanner(RowIndexedReader reader, int bufferSize) throws IOException
    {
        super();
        this.reader = reader;
        this.bufferSize = bufferSize;
        emptycf = reader.makeColumnFamily();
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
        row = null;
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
        chunkpos = 0;
        file.readInt(); // column count
        rowcolsoffset = file.getFilePointer();
    }

    /**
     * Un-positions a Scanner, so that it isn't pointed anywhere.
     */
    protected void clearPosition()
    {
        row = null;
        rowmeta = null;
    }

    @Override
    public void close() throws IOException
    {
        if (file != null)
            file.close();
    }

    @Override
    public long getFileLength()
    {
        try
        {
            if (file == null)
                first();
            return file.length();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    @Override
    public long getFilePointer()
    {
        if (file == null)
            return 0;
        return file.getFilePointer();
    }

    @Override
    public void first() throws IOException
    {
        repositionRow(0);
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
        return chunkpos < rowindex.size();
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
        return chunkpos < rowindex.size();
    }

    @Override
    public boolean hasNext()
    {
        if (file == null)
            return false;

        if (chunkpos+1 < rowindex.size())
            // more index chunks in the current row
            return true;
        if (rowdataoffset + rowlength < getFileLength())
            // more rows in the file
            return true;
        return false;
    }

    @Override
    public boolean next() throws IOException
    {
        assert file != null : "A Scanner must be positioned before use.";

        if (++chunkpos < rowindex.size())
            // more index chunks in the current row
            return true;
        if (rowdataoffset + rowlength < getFileLength())
        {
            // more rows in the file
            repositionRow(rowdataoffset + rowlength);
            return true;
        }
        return false;
    }

    @Override
    public Slice get()
    {
        if (rowmeta == null)
            return null;
        if (chunkpos >= rowindex.size())
            return null;
        
        // slices are inclusive,exclusive, but row indexes are inclusive,inclusive
        byte[] start = (chunkpos == 0) ?
            // first column of the chunk (rounded down to NAME_BEGIN for first chunk)
            ColumnKey.NAME_BEGIN : rowindex.get(chunkpos).firstName;
        byte[] end = (chunkpos < rowindex.size()-1) ?
            // first column of next chunk (rounded up to NAME_END for last chunk)
            rowindex.get(chunkpos+1).lastName : ColumnKey.NAME_END;
        return new Slice(rowmeta, new ColumnKey(rowkey, start), new ColumnKey(rowkey, end));
    }

    /**
     * protected hook to grab un-typed columns from disk.
     */
    protected List<IColumn> getRawColumns() throws IOException
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

    @Override
    public List<Column> getColumns() throws IOException
    {
        if (rowmeta == null)
            return null;
        
        // read sequential columns from the chunk
        ArrayList<Column> columns = new ArrayList<Column>();
        file.seek(rowcolsoffset + rowindex.get(chunkpos).offset);
        long chunkend = file.getFilePointer() + rowindex.get(chunkpos).width;
        while (file.getFilePointer() < chunkend)
            columns.add((Column)emptycf.getColumnSerializer().deserialize(file));
        return columns;
    }

    @Override
    public SliceBuffer getBuffer() throws IOException
    {
        Slice slice = get();
        if (slice == null)
            return null;

        // takes the easy way out, and deserializes the columns
        return new SliceBuffer(slice.meta, slice.begin, slice.end, getColumns());
    }

    @Override
    public IteratingRow getIteratingRow() throws IOException
    {
        if (file == null)
            return null;
        if (row != null)
        {
            row.skipRemaining();
        }
        else
        {
            // after first/seek*(), we won't be positioned at the beginning of the row
            file.seek(rowoffset);
        }
        if (file.getFilePointer() == file.length())
        {
            // end of file
            clearPosition();
            return null;
        }
        return row = new IteratingRow(file, reader);
    }
}
