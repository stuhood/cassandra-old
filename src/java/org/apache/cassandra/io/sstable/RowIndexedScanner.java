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
import org.apache.cassandra.db.filter.INameFilter;
import org.apache.cassandra.io.Slice;
import org.apache.cassandra.io.SliceBuffer;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In order to provide lazy behaviour, we implement hasNext and next directly rather than
 * via AbstractIterator (which eagerly calculates next).
 */
public class RowIndexedScanner implements SSTableScanner
{
    private static Logger logger = LoggerFactory.getLogger(RowIndexedScanner.class);

    private final RowIndexedReader reader;
    private final long length;
    private final int bufferSize;
    private BufferedRandomAccessFile file;
    private final ColumnFamily emptycf; // always empty: just a metadata holder
    protected final ColumnKey.Comparator comp;

    protected INameFilter filter = null;

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
    RowIndexedScanner(RowIndexedReader reader, int bufferSize)
    {
        super();
        this.reader = reader;
        this.length = reader.length();
        this.bufferSize = bufferSize;
        emptycf = reader.makeColumnFamily();
        this.comp = reader().getComparator();
    }
    
    @Override
    public int columnDepth()
    {
        return reader().getColumnDepth();
    }

    @Override
    public ColumnKey.Comparator comparator()
    {
        return comp;
    }

    @Override
    public void pushdownFilter(INameFilter filter)
    {
        this.filter = filter;
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
        rowkey = reader.getPartitioner().convertFromDiskFormat(FBUtilities.readShortByteArray(file));
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
    public boolean first()
    {
        try
        {
            repositionRow(0);
        }
        catch (IOException e)
        {
            return false;
        }
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
        assert file != null : "A Scanner must be positioned before use.";
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
    private SliceBuffer getChunkSlice() throws IOException
    {
        assert rowmeta != null;
        assert chunkpos < rowindex.size();
        
        // slices are inclusive,exclusive, but row indexes are inclusive,inclusive
        
        // first column of the chunk (rounded down to NAME_BEGIN for first chunk)
        byte[] beginb = (chunkpos == 0) ? ColumnKey.NAME_BEGIN : rowindex.get(chunkpos).firstName;
        ColumnKey begin = new ColumnKey(rowkey, beginb);
        // first column of next chunk (rounded up to NAME_END for last chunk)
        byte[] endb = (chunkpos < rowindex.size()-1) ? rowindex.get(chunkpos+1).lastName : ColumnKey.NAME_END;
        ColumnKey end = new ColumnKey(rowkey, endb);

        // either read or skip the columns
        return new SliceBuffer(rowmeta, begin, end, matchOrSkipSlice(begin, end));
    }

    /**
     * @return A populated or empty Column list, depending on filtering.
     */
    private List<Column> matchOrSkipSlice(ColumnKey begin, ColumnKey end) throws IOException
    {
        if (filter == null)
            return (List<Column>)getRawColumns();
        if (filter.mightMatchSlice(comp.comparatorAt(1), begin.name(1), end.name(1)))
            return (List<Column>)getFilteredRawColumns();
        return skipRawColumns();
    }

    /**
     * Pulls the columns in the current slice from disk, filtering them on the fly. Slices that are completely
     * eliminated via the filter will result in an empty list.
     * @return IColumns from disk for the current chunk of the index.
     */
    private List getFilteredRawColumns() throws IOException
    {
        file.seek(rowcolsoffset + rowindex.get(chunkpos).offset);
        long chunkend = file.getFilePointer() + rowindex.get(chunkpos).width;

        // filter individual columns
        Comparator<byte[]> ccomp = comp.comparatorAt(1);
        ArrayList<IColumn> columns = new ArrayList<IColumn>();
        while (file.getFilePointer() < chunkend)
        {
            IColumn col = emptycf.getColumnSerializer().deserialize(file);
            if (!filter.matchesName(ccomp, col.name()))
                continue;
            columns.add(col);
        }
        return columns;
    }

    /**
     * Pulls the columns in the current slice from disk.
     * @return IColumns from disk for the current chunk of the index.
     */
    protected List getRawColumns() throws IOException
    {
        // read sequential columns from the chunk
        file.seek(rowcolsoffset + rowindex.get(chunkpos).offset);
        long chunkend = file.getFilePointer() + rowindex.get(chunkpos).width;

        ArrayList<IColumn> columns = new ArrayList<IColumn>();
        while (file.getFilePointer() < chunkend)
            columns.add(emptycf.getColumnSerializer().deserialize(file));
        return columns;
    }

    /**
     * @return An empty list.
     */
    private List skipRawColumns() throws IOException
    {
        // no interesting columns in this slice
        file.seek(rowcolsoffset + rowindex.get(chunkpos).offset + rowindex.get(chunkpos).width);
        return Collections.<IColumn>emptyList();
    }
}
