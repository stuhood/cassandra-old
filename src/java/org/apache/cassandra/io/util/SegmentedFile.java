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

package org.apache.cassandra.io.util;

import java.io.*;
import java.util.*;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.MappedFileDataInput;

import com.google.common.collect.AbstractIterator;

/**
 * Abstracts a read-only file that has been split into segments, each of which can be represented by an independent
 * FileDataInput. Allows for iteration over the FileDataInputs, or random access to the FileDataInput for a given
 * position.
 *
 * The JVM can only map up to 2GB at a time, so each segment is at most that size when using mmap i/o. If a segment
 * would need to be longer than 2GB, that segment will not be mmap'd, and a new RandomAccessFile will be created for
 * each access to that segment.
 */
public class SegmentedFile
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentedFile.class);

    // in a perfect world, BUFFER_SIZE would be final, but we need to test with a smaller size to stay sane.
    static long BUFFER_SIZE = Integer.MAX_VALUE;

    public final String path;
    public final long length;

    /**
     * Map of segment offset to a MappedByteBuffer for that segment. If mmap is completely disabled, or if the segment
     * would be too long to mmap, the value for an offset will be null, indicating that we need to fall back to a
     * RandomAccessFile.
     */
    private final NavigableMap<Long, MappedByteBuffer> segments;

    /**
     * Use getBuilder to get a Builder to construct a SegmentedFile.
     */
    private SegmentedFile(String path, long length, NavigableMap<Long, MappedByteBuffer> segments)
    {
        this.path = path;
        this.length = length;
        this.segments = segments;
    }

    /**
     * @return A SegmentedFile.Builder.
     */
    public static Builder getBuilder()
    {
        if (DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap)
            return new MMAPBuilder();
        assert DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.standard;
        return new StandardBuilder();
    }

    /**
     * @return The segment containing the given position: must be closed after use.
     */
    public FileDataInput getSegment(long position, int bufferSize)
    {
        assert 0 <= position && position < length: position + " vs " + length;

        Map.Entry<Long, MappedByteBuffer> segment = segments.floorEntry(position);
        if (segment.getValue() != null)
        {
            // segment is mmap'd
            return new MappedFileDataInput(segment.getValue(), path, segment.getKey(), (int) (position - segment.getKey()));
        }

        // not mmap'd: open a braf covering the segment
        try
        {
            // FIXME: brafs are unbounded, so this segment will cover the rest of the file, rather than just the row
            BufferedRandomAccessFile file = new BufferedRandomAccessFile(path, "r", bufferSize);
            file.seek(position);
            return file;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * @return An Iterator over segments, beginning with the segment containing the given position: each segment must be closed after use.
     */
    public Iterator<FileDataInput> iterator(long position, int bufferSize)
    {
        return new SegmentIterator(position, bufferSize);
    }

    /**
     * A lazy Iterator over segments in forward order from the given position.
     */
    final class SegmentIterator implements Iterator<FileDataInput>
    {
        private long nextpos;
        private final int bufferSize;
        public SegmentIterator(long position, int bufferSize)
        {
            this.nextpos = position;
            this.bufferSize = bufferSize;;
        }
        
        @Override
        public boolean hasNext()
        {
            return nextpos < length;
        }

        @Override
        public FileDataInput next()
        {
            long position = nextpos;
            if (position >= length)
                throw new NoSuchElementException();

            FileDataInput segment = getSegment(nextpos, bufferSize);
            try
            {
                nextpos = segment.bytesRemaining();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            return segment;
        }
        
        @Override
        public void remove() { throw new UnsupportedOperationException(); }
    }

    /**
     * Collects potential segmentation points in an underlying file, and builds a SegmentedFile to represent it.
     */
    public abstract static class Builder
    {
        /**
         * Adds a position that would be a safe place for a segment boundary in the file. For a block/row based file
         * format, safe boundaries are block/row edges.
         * @param boundary The absolute position of the potential boundary in the file.
         */
        public abstract void addPotentialBoundary(long boundary);

        /**
         * Called after all potential boundaries have been added to apply this Builder to a concrete file on disk.
         * @param path The file on disk.
         */
        public abstract SegmentedFile complete(String path);
    }

    static final class MMAPBuilder extends Builder
    {
        // planned segment boundaries
        private final List<Long> boundaries;

        // offset of the open segment (first segment begins at 0).
        private long segoffset = 0;
        // current length of the open segment
        private long seglen = 0;

        public MMAPBuilder()
        {
            boundaries = new ArrayList<Long>();
            boundaries.add(Long.valueOf(0));
        }

        @Override
        public void addPotentialBoundary(long boundary)
        {
            // should this boundary close the current segment?
            if (BUFFER_SIZE <= boundary - segoffset)
            {
                if (seglen > 0)
                {
                    // close the current segment if it isn't empty
                    boundaries.add(segoffset + seglen);
                    segoffset += seglen;
                }
                seglen = boundary - segoffset;
            }

            if (BUFFER_SIZE <= seglen)
            {
                // still not enough room: dedicate an entire segment to this boundary
                boundaries.add(segoffset);
                segoffset = boundary;
                seglen = 0;
            }
        }

        @Override
        public SegmentedFile complete(String path)
        {
            long length = new File(path).length();
            // add a sentinel value == length
            boundaries.add(Long.valueOf(length));
            // create the segments
            return new SegmentedFile(path, length, createSegments(path, length));
        }

        private NavigableMap<Long, MappedByteBuffer> createSegments(String path, long length)
        {
            NavigableMap<Long, MappedByteBuffer> segments = new TreeMap<Long, MappedByteBuffer>();
            RandomAccessFile raf = null;
            try
            {
                raf = new RandomAccessFile(path, "r");
                int segcount = boundaries.size() - 1;
                for (int i = 0; i < segcount; i++)
                {
                    Long start = boundaries.get(i);
                    long size = boundaries.get(i + 1) - start;
                    if (BUFFER_SIZE < size)
                    {
                        // segment is too long to mmap
                        segments.put(start, null);
                        continue;
                    }

                    // mmap the segment
                    segments.put(start, raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size));
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                try
                {
                    if (raf != null) raf.close();
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
            return segments;
        }
    }

    static final class StandardBuilder extends Builder
    {
        public StandardBuilder() {}

        @Override
        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        @Override
        public SegmentedFile complete(String path)
        {
            long length = new File(path).length();
            NavigableMap<Long, MappedByteBuffer> segments = new TreeMap<Long, MappedByteBuffer>();
            segments.put(Long.valueOf(0), null);
            return new SegmentedFile(path, length, segments);
        }
    }
}
