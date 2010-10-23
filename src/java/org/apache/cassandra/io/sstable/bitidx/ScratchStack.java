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

package org.apache.cassandra.io.sstable.bitidx;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import org.apache.cassandra.io.sstable.bitidx.avro.*;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

/**
 * Maintains a stack of scratch files which are periodically merged.
 */
class ScratchStack
{
    private final Descriptor desc;
    private final Component.IdGenerator gen;
    // number of stack components of the same size to merge
    private final int mergeFactor;
    // stack of scratch components with their merge counts
    private final ArrayDeque<Pair<Integer,Component>> stack = new ArrayDeque<Pair<Integer,Component>>();
    public ScratchStack(Descriptor desc, Component.IdGenerator gen, int mergeFactor)
    {
        this.desc = desc;
        this.gen = gen;
        this.mergeFactor = mergeFactor;
    }

    /**
     * @param scratch A scratch component to add to the stack.
     */
    public void add(Component scratch) throws IOException
    {
        stack.push(new Pair<Integer,Component>(0, scratch));

        // merge while merges are available
        int depth;
        while ((depth = mergeNeeded()) > 0)
        {
            // merge the top mergeFactor scratch files to a new file
            Component dest = Component.get(Component.Type.SCRATCH, gen.getNextId());
            merge(mergeFactor, new Pair<Integer,Component>(depth, dest));
        }
    }

    /**
     * Finalizes this stack by merging all components to a single output component.
     * @param component The output component to merge stack contents to.
     */
    public void complete(Component output) throws IOException
    {
        if (stack.size() == 1)
            // already contains a single scratch component: just rename
            FileUtils.renameWithConfirm(new File(desc.filenameFor(stack.pop().right)),
                                        new File(desc.filenameFor(output)));
        else
        {
            // force all files to be merged to the destination
            merge(stack.size(), new Pair<Integer,Component>(-1, output));
            assert stack.size() == 1;
        }
    }

    /** @return A target depth to merge the top 'mergeFactor' components to, or 0 if not needed. */
    private int mergeNeeded()
    {
        int depth = 0;
        int sequential = 0;
        Iterator<Pair<Integer,Component>> iter = stack.iterator();
        if (iter.hasNext())
            depth = iter.next().left;
        while (iter.hasNext())
        {
            Pair<Integer,Component> component = iter.next();
            if (component.left != depth)
                // not enough components of this depth
                break;
            if (++sequential >= mergeFactor)
                // at least 'mergeFactor' components at this depth: return next depth
                return depth + 1;
        }
        return 0;
    }

    /**
     * Merges/replaces the top n components from the stack to/with the given output.
     */
    private void merge(int n, Pair<Integer,Component> output) throws IOException
    {
        /**
         * Open readers for the top n components and merge them. Files are stacked in order by
         * rowid/creation time, and all of them contain the same bins, so we empty the segments
         * for each bin from the readers in order, outputting sorted bins containing sorted rowids.
         */
        List<File> files = popTopNFiles(n);
        List<DataFileReader<BinSegment>> readers = new ArrayList<DataFileReader<BinSegment>>();
        for (File file : files)
            readers.add(new DataFileReader<BinSegment>(file, new SpecificDatumReader<BinSegment>()));

        // get metadata from the first reader to apply to the output
        byte[] indexMetadata = readers.get(0).getMeta(BitmapIndex.META_KEY);

        // open the output
        DataFileWriter writer = new DataFileWriter(new SpecificDatumWriter<BinSegment>());
        writer.setMeta(BitmapIndex.META_KEY, indexMetadata);
        writer.create(BinSegment.SCHEMA$, new File(desc.filenameFor(output.right)));
        ReusableBinSegment segment = ReusableBinSegment.poolGet();
        try
        {
            // loop over bins: all readers will reach eof after the same bin
            boolean eof = false;
            int headersSeen = 0;
            long cardinality = 0;
            while (!eof)
            {
                // for each reader in order, write out all data segments for this bin
                for (DataFileReader<BinSegment> reader : readers)
                {
                    while (reader.hasNext())
                    {
                        segment.readFrom(reader);
                        if (segment.data != null)
                            // data segment: write to output
                            writer.append(segment);
                        else
                        {
                            // header segment: reached end of bin for this reader
                            cardinality += segment.header.cardinality;
                            if (++headersSeen == n)
                            {
                                // write once all headers have been seen for a bin
                                segment.header.cardinality = cardinality;
                                writer.append(segment);
                                writer.sync();
                            }
                            break;
                        }
                    }
                    if (!reader.hasNext())
                        // reached the last bin: finish emptying readers
                        
                        eof = true;
                }
                // reset for the next bin
                headersSeen = 0;
                cardinality = 0;
            }
        }
        finally
        {
            ReusableBinSegment.poolReturn(segment);
            writer.close();
            for (DataFileReader<BinSegment> reader : readers)
                reader.close();
        }
        // add the newly written component to the stack
        stack.push(output);
        // cleanup the retired readers
        for (File file : files)
            FileUtils.deleteWithConfirm(file);
    }
    
    /**
     * @return The top n files from the stack, in ascending rowid order.
     */
    private List<File> popTopNFiles(int n)
    {
        List<File> files = new ArrayList<File>(n);
        for (; n > 0; n--)
            files.add(new File(desc.filenameFor(stack.pop().right)));
        Collections.reverse(files);
        return files;
    }
}
