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
package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;

import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class SSTableUtils
{
    // first configured table and cf
    public static String TABLENAME;
    public static String CFNAME;
    static
    {
        try
        {
            TABLENAME = DatabaseDescriptor.getTables().get(0);
            CFNAME = Table.open(TABLENAME).getColumnFamilies().iterator().next();
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
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
        return File.createTempFile(cfname + "-",
                                   "-" + SSTable.TEMPFILE_MARKER + "-Data.db",
                                   tabledir);
    }

    public static SSTableReader writeSSTable(Set<String> keys) throws IOException
    {
        TreeMap<String, ColumnFamily> map = new TreeMap<String, ColumnFamily>();
        for (String key : keys)
        {
            ColumnFamily cf = ColumnFamily.create(TABLENAME, CFNAME);
            cf.addColumn(new Column(key.getBytes(), key.getBytes(), 0));
            map.put(key, cf);
        }
        return writeSSTable(map);
    }

    public static SSTableReader writeSSTable(SortedMap<String, ColumnFamily> entries) throws IOException
    {
        TreeMap<ColumnKey, Column> map =
            new TreeMap<ColumnKey, Column>(ColumnKey.getComparator(TABLENAME, CFNAME));
        for (Map.Entry<String, ColumnFamily> entry : entries.entrySet())
        {
            DecoratedKey key = StorageService.getPartitioner().decorateKey(entry.getKey());

            // flatten the column family into columns
            for (IColumn col : entry.getValue().getSortedColumns())
            {
                if (!entry.getValue().isSuper())
                {
                    map.put(new ColumnKey(key, col.name()), (Column)col);
                    continue;
                }

                SuperColumn supercol = (SuperColumn)col;
                for (IColumn subcol : supercol.getSubColumns())
                {
                    map.put(new ColumnKey(key, supercol.name(), subcol.name()), (Column)subcol);
                }
            }
        }
        return writeRawSSTable(TABLENAME, CFNAME, map);
    }

    /**
     * Writes a series of columns which may fall into many slices. All columns will
     * have the same empty metadata.
     */
    public static SSTableReader writeRawSSTable(String tablename, String cfname, SortedMap<ColumnKey, Column> entries) throws IOException
    {
        File f = tempSSTableFile(tablename, cfname);
        SSTableWriter writer = new SSTableWriter(f.getAbsolutePath(), entries.size(), StorageService.getPartitioner());

        Slice.Metadata meta = new Slice.Metadata();
        for (Map.Entry<ColumnKey, Column> entry : entries.entrySet())
        {
            writer.append(meta, entry.getKey(), entry.getValue());
        }
        new File(writer.indexFilename()).deleteOnExit();
        new File(writer.filterFilename()).deleteOnExit();
        return writer.closeAndOpenReader(1.0);
    }
}
