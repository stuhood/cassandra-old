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
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.AbstractCompactedRow;
import org.apache.cassandra.io.ColumnObserver;
import org.apache.cassandra.io.ICompactionInfo;
import org.apache.cassandra.io.sstable.bitidx.BitmapIndexWriter;
import org.apache.cassandra.io.sstable.bitidx.BitmapIndexReader;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// yuck
import org.apache.cassandra.thrift.IndexType;

public class SSTableWriter extends SSTable
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final BufferedRandomAccessFile dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;

    public SSTableWriter(String filename, long keyCount) throws IOException
    {
        this(filename, keyCount, DatabaseDescriptor.getCFMetaData(Descriptor.fromFilename(filename)), StorageService.getPartitioner());
    }

    public SSTableWriter(String filename, long keyCount, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        super(Descriptor.fromFilename(filename), new HashSet<Component>(), metadata, partitioner, SSTable.defaultRowHistogram(), SSTable.defaultColumnHistogram());
        iwriter = new IndexWriter(descriptor, metadata, partitioner, keyCount, Component.INDEX_TYPES);
        dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        dataFile = new BufferedRandomAccessFile(getFilename(), "rw", DatabaseDescriptor.getInMemoryCompactionLimit());
    }
    
    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void reset()
    {
        try
        {
            dataFile.reset(dataMark);
            iwriter.reset();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + getFilename());
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition) throws IOException
    {
        lastWrittenKey = decoratedKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        iwriter.afterAppend(decoratedKey, dataPosition);
        dbuilder.addPotentialBoundary(dataPosition);
    }

    public void append(AbstractCompactedRow row) throws IOException
    {
        long currentPosition = beforeAppend(row.key);
        FBUtilities.writeShortByteArray(row.key.key, dataFile);
        row.write(dataFile, iwriter.observers);
        estimatedRowSize.add(dataFile.getFilePointer() - currentPosition);
        estimatedColumnCount.add(row.columnCount());
        afterAppend(row.key, currentPosition);
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        FBUtilities.writeShortByteArray(decoratedKey.key, dataFile);
        // write placeholder for the row size, since we don't know it yet
        long sizePosition = dataFile.getFilePointer();
        dataFile.writeLong(-1);
        // allow observers to observe content
        for (ColumnObserver observer : iwriter.observers)
            observer.maybeObserve(cf);
        // write out row data
        int columnCount = ColumnFamily.serializer().serializeWithIndexes(cf, dataFile);
        // seek back and write the row size (not including the size Long itself)
        long endPosition = dataFile.getFilePointer();
        dataFile.seek(sizePosition);
        dataFile.writeLong(endPosition - (sizePosition + 8));
        // finally, reset for next row
        dataFile.seek(endPosition);
        afterAppend(decoratedKey, startPosition);
        estimatedRowSize.add(endPosition - startPosition);
        estimatedColumnCount.add(columnCount);
    }

    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        // FIXME: terrible hack (but BMT is a terrible hack... they deserve eachother)
        if (!iwriter.observers.isEmpty())
            throw new RuntimeException("FIXME: Secondary indexing not supported with BMT.");

        long currentPosition = beforeAppend(decoratedKey);
        FBUtilities.writeShortByteArray(decoratedKey.key, dataFile);
        assert value.remaining() > 0;
        dataFile.writeLong(value.remaining());
        dataFile.write(value.array(),value.position()+value.arrayOffset(),value.remaining());
        afterAppend(decoratedKey, currentPosition);
    }

    public SSTableReader closeAndOpenReader() throws IOException
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge) throws IOException
    {
        // index and filter
        iwriter.close();
        // main data
        long position = dataFile.getFilePointer();
        dataFile.close(); // calls force
        FileUtils.truncate(dataFile.getPath(), position);

        // write sstable statistics
        writeStatistics(descriptor, estimatedRowSize, estimatedColumnCount);

        // determine the components we've written
        HashSet<Component> wcomponents = new HashSet<Component>();
        wcomponents.add(Component.DATA);
        wcomponents.add(Component.STATS);
        wcomponents.addAll(iwriter.components);
        // remove the 'tmp' marker from all components
        final Descriptor newdesc = rename(descriptor, wcomponents);

        // open readers for each secondary index
        Map<ByteBuffer,BitmapIndexReader> secindexes = new TreeMap<ByteBuffer,BitmapIndexReader>(metadata.comparator);
        for (BitmapIndexWriter secindex : iwriter.secindexes)
            secindexes.put(secindex.name(), BitmapIndexReader.open(newdesc, secindex.component));

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.getPrimaryIndexBuilder().complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));

        SSTableReader sstable = SSTableReader.internalOpen(newdesc, wcomponents, metadata, partitioner, ifile, dfile, iwriter.getPrimaryIndexSummary(), iwriter.getBF(), maxDataAge, estimatedRowSize, estimatedColumnCount, secindexes);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    private static void writeStatistics(Descriptor desc, EstimatedHistogram rowSizes, EstimatedHistogram columnnCounts) throws IOException
    {
        DataOutputStream out = new DataOutputStream(new FileOutputStream(desc.filenameFor(SSTable.COMPONENT_STATS)));
        EstimatedHistogram.serializer.serialize(rowSizes, out);
        EstimatedHistogram.serializer.serialize(rowSizes, out);
        out.close();
    }

    static Descriptor rename(Descriptor tmpdesc, Set<Component> components)
    {
        Descriptor newdesc = tmpdesc.asTemporary(false);
        try
        {
            for (Component component : components)
                FBUtilities.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return newdesc;
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }
    
    public static Builder createBuilder(ColumnFamilyStore cfs, Descriptor desc, Set<Component.Type> ctypes)
    {
        if (!desc.isLatestVersion)
            // TODO: streaming between different versions will fail: need support for
            // recovering other versions to provide a stable streaming api
            throw new RuntimeException(String.format("Cannot recover SSTable with version %s (current version %s).",
                                                     desc.version, Descriptor.CURRENT_VERSION));

        return new Builder(cfs, desc, ctypes);
    }

    /**
     * Removes the given SSTable from temporary status and opens it, rebuilding the
     * given Component types from the data file.
     */
    public static class Builder implements ICompactionInfo
    {
        private final ColumnFamilyStore cfs;
        private final Descriptor desc;
        private final Set<Component.Type> ctypes;
        private BufferedRandomAccessFile dfile;

        public Builder(ColumnFamilyStore cfs, Descriptor desc, Set<Component.Type> ctypes)
        {
            this.cfs = cfs;
            this.desc = desc;
            this.ctypes = ctypes;
            try
            {
                dfile = new BufferedRandomAccessFile(desc.filenameFor(SSTable.COMPONENT_DATA), "r", 8 * 1024 * 1024);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public Pair<Descriptor,Set<Component>> build() throws IOException
        {
            if (cfs.isInvalid())
                return null;
            logger.debug("Rebuilding {} for {}", ctypes, desc);
            EstimatedHistogram rowSizes = SSTable.defaultRowHistogram();
            EstimatedHistogram columnCounts = SSTable.defaultColumnHistogram();

            IndexWriter iwriter;
            long estimatedRows;
            try
            {
                estimatedRows = SSTable.estimateRowsFromData(desc, dfile);
                iwriter = new IndexWriter(desc, cfs.metadata, StorageService.getPartitioner(), estimatedRows, ctypes);
            }
            catch(IOException e)
            {
                dfile.close();
                throw e;
            }

            // request deserialization of names for any secondary indexes
            SortedSet<ByteBuffer> observed = new TreeSet<ByteBuffer>(cfs.metadata.comparator);
            for (ColumnObserver observer : iwriter.observers)
                observed.add(observer.name);

            // build the primary index, filter, and any secondary indexes
            long rows = 0;
            try
            {
                DecoratedKey key;
                long rowPosition = 0;
                while (rowPosition < dfile.length())
                {
                    key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, FBUtilities.readShortByteArray(dfile));
                    long dataSize = SSTableReader.readRowSize(dfile, desc);
                    long nextRowPosition = dfile.getFilePointer() + dataSize;
                    rowSizes.add(dataSize);

                    if (!observed.isEmpty())
                    {
                        // deserialize and observe interesting columns
                        SSTableNamesIterator sstni = new SSTableNamesIterator(cfs.metadata, dfile, key, observed);
                        Iterator<IColumn> iter = ColumnObserver.Iterator.apply(sstni, iwriter.observers);
                        // consume filtered columns
                        while (iter.hasNext())
                            iter.next();
                        columnCounts.add(sstni.getTotalColumns());
                    }
                    else
                    {
                        // deserialize only enough to determine the column count
                        IndexHelper.skipBloomFilter(dfile);
                        IndexHelper.skipIndex(dfile);
                        ColumnFamily.serializer().deserializeFromSSTableNoColumns(ColumnFamily.create(cfs.metadata), dfile);
                        columnCounts.add(dfile.readInt());
                    }

                    iwriter.afterAppend(key, rowPosition);
                    dfile.seek(nextRowPosition);
                    rowPosition = nextRowPosition;
                    rows++;
                }

                if (ctypes.contains(Component.STATS))
                    writeStatistics(desc, rowSizes, columnCounts);
            }
            finally
            {
                try
                {
                    dfile.close();
                    iwriter.close();
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }

            logger.debug("estimated row count was {} of real count", ((double)estimatedRows) / rows);
            // TODO: shouldn't need to search the directory to find created components
            Set<Component> components = SSTable.componentsFor(desc);
            return new Pair<Descriptor,Set<Component>>(rename(desc, components), components);
        }

        public long getTotalBytes()
        {
            try
            {
                return dfile.length();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public long getBytesRead()
        {
            return dfile.getFilePointer();
        }

        public String getTaskType()
        {
            return "SSTable rebuild for " + ctypes;
        }
    }

    /**
     * Encapsulates writing index-like components for an SSTable. The state of this object is not
     * valid until it has been closed. Typically, an IndexWriter will be controlled by a Builder
     * (during recovery) or by an SSTableWriter directly (during flush/compaction).
     * TODO: ripe for composition
     */
    static class IndexWriter
    {
        // metadata
        public final Descriptor desc;
        public final IPartitioner partitioner;
        public final Set<Component.Type> types;
        public final Set<Component> components;

        // state
        private boolean closed = false;
        private BufferedRandomAccessFile indexFile;
        private SegmentedFile.Builder builder;
        private FileMark indexMark;
        private IndexSummary summary;
        private BloomFilter bf;
        private TreeSet<ColumnObserver> observers;
        private ArrayList<BitmapIndexWriter> secindexes;
        
        IndexWriter(Descriptor desc, CFMetaData metadata, IPartitioner part, long keyCount, Set<Component.Type> types) throws IOException
        {
            this.desc = desc;
            this.partitioner = part;
            this.types = types;
            this.components = new HashSet<Component>();

            // determine and initialize the components to write
            for (Component.Type ctype : types)
            {
                switch (ctype)
                {
                    case PRIMARY_INDEX:
                        components.add(Component.get(ctype));
                        File ifile = new File(desc.filenameFor(Component.PRIMARY_INDEX));
                        assert !ifile.exists();
                        indexFile = new BufferedRandomAccessFile(ifile, "rw", 8 * 1024 * 1024);
                        builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
                        summary = new IndexSummary(keyCount);
                        break;
                    case FILTER:
                        components.add(Component.get(ctype));
                        assert !(new File(desc.filenameFor(Component.FILTER)).exists());
                        bf = BloomFilter.getFilter(keyCount, 15);
                        break;
                    case BITMAP_INDEX:
                        Component.IdGenerator gen = new Component.IdGenerator();
                        secindexes = new ArrayList<BitmapIndexWriter>();
                        observers = new TreeSet<ColumnObserver>();
                        // open writers/components for each bitmap secondary index
                        for (ColumnDefinition cdef : metadata.getColumn_metadata().values())
                        {
                            if (cdef.getIndexType() != IndexType.KEYS_BITMAP)
                                continue;

                            // assign a component id, and open a writer for the index
                            BitmapIndexWriter bmiw = new BitmapIndexWriter(desc, gen, cdef, metadata.comparator);
                            components.add(bmiw.component);
                            observers.add(bmiw.observer);
                            secindexes.add(bmiw);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("IndexWriter cannot write component type " + ctype);
                }
            }
        }

        public IndexSummary getPrimaryIndexSummary()
        {
            assert closed; return summary;
        }

        public SegmentedFile.Builder getPrimaryIndexBuilder()
        {
            assert closed; return builder;
        }

        public BloomFilter getBF()
        {
            assert closed; return bf;
        }

        public void afterAppend(DecoratedKey key, long dataPosition) throws IOException
        {
            if (types.contains(Component.Type.FILTER))
            {
                bf.add(key.key);
            }
            if (types.contains(Component.Type.PRIMARY_INDEX))
            {
                long indexPosition = indexFile.getFilePointer();
                FBUtilities.writeShortByteArray(key.key, indexFile);
                indexFile.writeLong(dataPosition);
                summary.maybeAddEntry(key, indexPosition);
                builder.addPotentialBoundary(indexPosition);
                if (logger.isTraceEnabled())
                    logger.trace("wrote index of " + key + " at " + indexPosition);
            }
            if (types.contains(Component.Type.BITMAP_INDEX))
            {
                for (BitmapIndexWriter secindex : secindexes)
                    secindex.incrementRowId();
            }
        }

        /**
         * Close all components, making the public state of this writer valid for consumption.
         */
        public void close() throws IOException
        {
            if (types.contains(Component.Type.FILTER))
            {
                FileOutputStream fos = new FileOutputStream(desc.filenameFor(SSTable.COMPONENT_FILTER));
                DataOutputStream stream = new DataOutputStream(fos);
                BloomFilter.serializer().serialize(bf, stream);
                stream.flush();
                fos.getFD().sync();
                stream.close();
            }
            if (types.contains(Component.Type.PRIMARY_INDEX))
            {
                indexFile.getChannel().force(true);
                long position = indexFile.getFilePointer();
                indexFile.close();
                // truncate any junk data from failed mark/reset
                FileUtils.truncate(indexFile.getPath(), position);
                summary.complete();
            }
            if (types.contains(Component.Type.BITMAP_INDEX))
            {
                for (BitmapIndexWriter secindex : secindexes)
                    secindex.close();
            }

            closed = true;
        }

        public void mark()
        {
            if (types.contains(Component.Type.PRIMARY_INDEX))
            {
                indexMark = indexFile.mark();
            }
        }

        public void reset() throws IOException
        {
            if (types.contains(Component.Type.PRIMARY_INDEX))
            {
                // we can't un-set the bloom filter addition, but extra keys in there are harmless.
                // we can't reset dbuilder either, but that is the last thing called in afterappend so
                // we assume that if that worked then we won't be trying to reset.
                indexFile.reset(indexMark);
            }
            if (types.contains(Component.Type.BITMAP_INDEX))
            {
                // clear the current bit
                for (BitmapIndexWriter secindex : secindexes)
                    secindex.reset();
            }
        }
    }
}
