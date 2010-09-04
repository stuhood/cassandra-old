/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataOutputBuffer;

import org.apache.cassandra.Util;

public class SSTableUtils
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableUtils.class);
    
    // first configured table and cf
    public static String TABLENAME = "Keyspace1";
    public static String CFNAME = "Standard1";

    public static ColumnFamily createCF(long mfda, int ldt, IColumn... cols)
    {
        ColumnFamily cf = ColumnFamily.create(TABLENAME, CFNAME);
        cf.delete(ldt, mfda);
        for (IColumn col : cols)
            cf.addColumn(col);
        return cf;
    }

    public static File tempSSTableFile(String tablename, String cfname) throws IOException
    {
        File tempdir = File.createTempFile(tablename, cfname);
        if(!tempdir.delete() || !tempdir.mkdir())
            throw new IOException("Temporary directory creation failed.");
        tempdir.deleteOnExit();
        File tabledir = new File(tempdir, tablename);
        tabledir.mkdir();
        tabledir.deleteOnExit();
        File datafile = new File(new Descriptor(tabledir, tablename, cfname, 0, false).filenameFor("Data.db"));
        if (!datafile.createNewFile())
            throw new IOException("unable to create file " + datafile);
        datafile.deleteOnExit();
        return datafile;
    }

    public static SSTableReader writeSSTable(Set<String> keys) throws IOException
    {
        return writeSSTable(TABLENAME, CFNAME, keys);
    }

    public static SSTableReader writeSSTable(String ksname, String cfname, Set<String> keys) throws IOException
    {
        Map<String, ColumnFamily> map = new HashMap<String, ColumnFamily>();
        for (String key : keys)
        {
            ColumnFamily cf = ColumnFamily.create(ksname, cfname);
            cf.addColumn(new Column(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(key.getBytes()), 0));
            map.put(key, cf);
        }
        return writeSSTable(ksname, cfname, map);
    }

    public static SSTableReader writeSSTable(Map<String, ColumnFamily> entries) throws IOException
    {
        return writeSSTable(TABLENAME, CFNAME, entries);
    }

    public static SSTableReader writeSSTable(String ksname, String cfname, Map<String, ColumnFamily> entries) throws IOException
    {
        SortedMap<DecoratedKey, ColumnFamily> sorted = new TreeMap<DecoratedKey, ColumnFamily>();
        for (Map.Entry<String, ColumnFamily> entry : entries.entrySet())
            sorted.put(Util.dk(entry.getKey()), entry.getValue());

        final Iterator<Map.Entry<DecoratedKey, ColumnFamily>> iter = sorted.entrySet().iterator();
        return writeSSTable(ksname, cfname, sorted.size(), new Appender()
        {
            @Override
            public boolean append(SSTableWriter writer) throws IOException
            {
                if (!iter.hasNext()) return false;
                Map.Entry<DecoratedKey, ColumnFamily> entry = iter.next();
                writer.append(entry.getKey(), entry.getValue());
                return true;
            }   
        });
    }

    public static SSTableReader writeRawSSTable(String tablename, String cfname, Map<ByteBuffer, ByteBuffer> entries) throws IOException
    {
        SortedMap<DecoratedKey, ByteBuffer> sorted = new TreeMap<DecoratedKey, ByteBuffer>();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet())
            sorted.put(Util.dk(entry.getKey()), entry.getValue());

        final Iterator<Map.Entry<DecoratedKey, ByteBuffer>> iter = sorted.entrySet().iterator();
        return writeSSTable(tablename, cfname, sorted.size(), new Appender()
        {
            @Override
            public boolean append(SSTableWriter writer) throws IOException
            {
                if (!iter.hasNext()) return false;
                Map.Entry<DecoratedKey, ByteBuffer> entry = iter.next();
                writer.append(entry.getKey(), entry.getValue());
                return true;
            }
        });
    }

    public static SSTableReader writeSSTable(String tablename, String cfname, int size, Appender appender) throws IOException
    {
        File datafile = tempSSTableFile(tablename, cfname);
        SSTableWriter writer = new SSTableWriter(datafile.getAbsolutePath(), size);
        long start = System.currentTimeMillis();
        while (appender.append(writer)) { /* pass */ }
        SSTableReader reader = writer.closeAndOpenReader();
        logger.info("In {} ms, wrote {}", System.currentTimeMillis() - start, writer);
        // mark all components for removal
        for (Component component : reader.components)
            new File(reader.descriptor.filenameFor(component)).deleteOnExit();
        return reader;
    }

    /** Inverts control, so that callers decide how to append to the SSTableWriter */ 
    static abstract class Appender
    {
        /**
         * Called with an open writer until it returns false.
         */
        abstract boolean append(SSTableWriter writer) throws IOException;
    }
}
