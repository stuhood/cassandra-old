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
import org.apache.cassandra.db.DecoratedKey;

/**
 * This class is built on top of the SequenceFile. It stores
 * data on disk in sorted fashion. However the sorting is upto
 * the application. This class expects keys to be handed to it
 * in sorted order.
 *
 * A separate index file is maintained as well, containing the
 * SSTable keys and the offset into the SSTable at which they are found.
 * Every 1/indexInterval key is read into memory when the SSTable is opened.
 *
 * Finally, a bloom filter file is also kept for the keys in each SSTable.
 */
public abstract class SSTable
{
    private static final Logger logger = Logger.getLogger(SSTable.class);

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
        this.comparator = ColumnKey.getComparator(getTableName(), getColumnFamilyName());;
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
        return getClass().getName() + "(" +
               "path='" + path + '\'' +
               ')';
    }

    /**
     * A marker used in the SSTable data file to delineate slices, store shared
     * metadata about those slices, and mark the end of blocks.
     */
    static class SliceMark
    {
        // a mark with this status indicates the end of a block: the mark should
        // contain the last key in the current block, and the first key of the next
        // block. if this was the last block in the file, then nextKey will be null
        public static final int BLOCK_END = -1;


        public final ColumnKey currentKey;
        public final ColumnKey nextKey;
        // uncompressed bytes to next SliceMark, or a negative status value
        public final int nextMark;
        public final Slice.Metadata parentMeta;

        /**
         * Create a mark with empty metadata.
         */
        public SliceMark(ColumnKey currentKey, ColumnKey nextKey, int nextMark)
        {
            this(new Slice.Metadata(), currentKey, nextKey, nextMark);
        }

        public SliceMark(Slice.Metadata parentMeta, ColumnKey currentKey, ColumnKey nextKey, int nextMark)
        {
            assert parentMeta.size() < Byte.MAX_VALUE;
            assert currentKey != null;
            this.currentKey = currentKey;
            this.nextKey = nextKey;
            this.nextMark = nextMark;
            this.parentMeta = parentMeta;
        }

        public void serialize(DataOutput dos) throws IOException
        {
            currentKey.serialize(dos);
            // nextKey is nullable, indicating the end of the file
            dos.writeBoolean(nextKey != null);
            if (nextKey != null)
                nextKey.serialize(dos);

            dos.writeInt(nextMark);
            parentMeta.serialize(dos);
        }

        public static SliceMark deserialize(DataInput dis) throws IOException
        {
            ColumnKey currentKey = ColumnKey.deserialize(dis);
            ColumnKey nextKey = dis.readBoolean() ? ColumnKey.deserialize(dis) : null;
            int nextMark = dis.readInt();
            Slice.Metadata parentMeta = Slice.Metadata.deserialize(dis);
            return new SliceMark(parentMeta, currentKey, nextKey, nextMark);
        }

        public String toString()
        {
            return "#<SliceMark " + currentKey + " " + nextKey + " " + nextMark + ">";
        }
    }
}
