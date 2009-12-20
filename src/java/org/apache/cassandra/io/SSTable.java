package org.apache.cassandra.io;
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


import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.ColumnKey;

/**
 * This class stores columns on disk in sorted fashion. See SSTableWriter for
 * a more thorough explanation of the file format.
 *
 * A separate index file is maintained, containing the offsets of SSTable blocks.
 * Every 1/indexInterval index entry is read into memory when the SSTable is opened.
 *
 * Finally, a bloom filter file is also kept for the keys in each SSTable.
 */
public abstract class SSTable
{
    private static final Logger logger = Logger.getLogger(SSTable.class);

    /**
     * The version of the SSTable file format that can be read and written using
     * this class: attempting to open an sstable version that does not match will
     * raise an assertion.
     */
    public static final byte VERSION = (byte)1;

    public static final int FILES_ON_DISK = 3; // data, index, and bloom filter

    protected String path;
    protected final IPartitioner partitioner;
    protected BloomFilter bf;
    protected List<IndexEntry> indexEntries;
    protected final String columnFamilyName;
    protected final ColumnKey.Comparator comparator;

    /**
     * Every INDEX_INTERVALth index entry is loaded into memory so we know where
     * to start looking for the IndexEntry on disk with less seeking. The index
     * contains one IndexEntry per block.
     *
     * FIXME: make configurable
     */
    public static final int INDEX_INTERVAL = 16;

    /* Required extension for temporary files created during compactions. */
    public static final String TEMPFILE_MARKER = "tmp";

    public SSTable(String filename, IPartitioner partitioner)
    {
        assert filename.endsWith("-Data.db");
        columnFamilyName = new File(filename).getName().split("-")[0];
        this.path = filename;
        this.partitioner = partitioner;
        this.indexEntries = new ArrayList<IndexEntry>();
        this.comparator = ColumnKey.getComparator(getTableName(), getColumnFamilyName());
    }

    protected static String indexFilename(String dataFile)
    {
        String[] parts = dataFile.split("-");
        parts[parts.length - 1] = "Index.db";
        return StringUtils.join(parts, "-");
    }

    public String indexFilename()
    {
        return indexFilename(path);
    }

    protected static String compactedFilename(String dataFile)
    {
        String[] parts = dataFile.split("-");
        parts[parts.length - 1] = "Compacted";
        return StringUtils.join(parts, "-");
    }

    /**
     * We use a ReferenceQueue to manage deleting files that have been compacted
     * and for which no more SSTable references exist.  But this is not guaranteed
     * to run for each such file because of the semantics of the JVM gc.  So,
     * we write a marker to `compactedFilename` when a file is compacted;
     * if such a marker exists on startup, the file should be removed.
     *
     * @return true if the file was deleted
     */
    public static boolean deleteIfCompacted(String dataFilename) throws IOException
    {
        if (new File(compactedFilename(dataFilename)).exists())
        {
            delete(dataFilename);
            return true;
        }
        return false;
    }

    protected String compactedFilename()
    {
        return compactedFilename(path);
    }

    protected static String filterFilename(String dataFile)
    {
        String[] parts = dataFile.split("-");
        parts[parts.length - 1] = "Filter.db";
        return StringUtils.join(parts, "-");
    }

    public String filterFilename()
    {
        return filterFilename(path);
    }

    public String getFilename()
    {
        return path;
    }

    /** @return full paths to all the files associated w/ this SSTable */
    public List<String> getAllFilenames()
    {
        // TODO streaming relies on the -Data (getFilename) file to be last, this is clunky
        return Arrays.asList(indexFilename(), filterFilename(), getFilename());
    }

    public ColumnKey.Comparator getComparator()
    {
        return comparator;
    }

    public String getColumnFamilyName()
    {
        return columnFamilyName;
    }

    public String getTableName()
    {
        return parseTableName(path);
    }

    public static String parseTableName(String filename)
    {
        return new File(filename).getParentFile().getName();        
    }

    static void delete(String path) throws IOException
    {
        FileUtils.deleteWithConfirm(new File(path));
        FileUtils.deleteWithConfirm(new File(SSTable.indexFilename(path)));
        FileUtils.deleteWithConfirm(new File(SSTable.filterFilename(path)));
        FileUtils.deleteWithConfirm(new File(SSTable.compactedFilename(path)));
        logger.info("Deleted " + path);
    }

    public long bytesOnDisk()
    {
        long bytes = 0;
        for (String fname : getAllFilenames())
        {
            bytes += new File(fname).length();
        }
        return bytes;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "(" + "path='" + path + "')";
    }

    /**
     * A marker used in the SSTable data file to delineate slices, and store shared
     * metadata about those slices. The status codes in SliceMarks describe the
     * position of the slice in the block.
     */
    static class SliceMark extends Slice
    {
        // there are more slices in the block
        public static final byte BLOCK_CONTINUE = (byte)0;
        // this is the last slice in the block
        public static final byte BLOCK_END = (byte)-1;

        // uncompressed bytes to next SliceMark
        public final int length;
        // number of columns in the slice
        public final int numCols;
        // status of this slice in the current block
        public final byte status;

        public SliceMark(Slice.Metadata meta, ColumnKey currentKey, ColumnKey nextKey, int length, int numCols, byte status)
        {
            super(meta, currentKey, nextKey);
            this.length = length;
            this.numCols = numCols;
            this.status = status;
        }

        public void serialize(DataOutput dos) throws IOException
        {
            currentKey.serialize(dos);
            // nextKey is nullable, indicating the end of the file
            dos.writeBoolean(nextKey != null);
            if (nextKey != null)
                nextKey.serialize(dos);
            meta.serialize(dos);

            dos.writeInt(length);
            dos.writeInt(numCols);
            dos.writeByte(status);
        }

        public static SliceMark deserialize(DataInput dis) throws IOException
        {
            ColumnKey currentKey = ColumnKey.deserialize(dis);
            ColumnKey nextKey = dis.readBoolean() ? ColumnKey.deserialize(dis) : null;
            Slice.Metadata meta = Slice.Metadata.deserialize(dis);

            int length = dis.readInt();
            int numCols = dis.readInt();
            byte status = dis.readByte();
            return new SliceMark(meta, currentKey, nextKey,
                                 length, numCols, status);
        }

        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            buff.append("#<SliceMark ").append(currentKey).append(" (");
            buff.append(numCols).append(") ").append(nextKey).append(">");
            return buff.toString();
        }
    }

    /**
     * Plays the role of header for a block. BlockHeaders lie before the portion of a
     * block that might be compressed, and are used to store compression and version
     * info.
     */
    static class BlockHeader
    {
        public static final int MAGIC = 1337;

        // compression codec
        public final String codecClass;

        public BlockHeader(String codecClass)
        {
            this.codecClass = codecClass;
        }

        public void serialize(DataOutput dos) throws IOException
        {
            dos.writeInt(MAGIC);
            dos.writeByte(VERSION);
            dos.writeUTF(codecClass);
        }

        public static BlockHeader deserialize(DataInput dis) throws IOException
        {
            assert MAGIC == dis.readInt() && VERSION == dis.readByte():
                "A corrupt or outdated SSTable was detected.";
            return new BlockHeader(dis.readUTF());
        }
    }
}
