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
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.db.DecoratedKey;

public class RowIndexedScannerTest extends RowIndexedTestBase
{
    protected void verifySingle(RowIndexedReader sstable, byte[] bytes, String key) throws IOException
    {
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)).position);
        assert key.equals(file.readUTF());
        int size = file.readInt();
        byte[] bytes2 = new byte[size];
        file.readFully(bytes2);
        assert Arrays.equals(bytes2, bytes);
    }

    protected void verifyMany(RowIndexedReader sstable, TreeMap<String, byte[]> map) throws IOException
    {
        List<String> keys = new ArrayList<String>(map.keySet());
        Collections.shuffle(keys);
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        for (String key : keys)
        {
            file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)).position);
            assert key.equals(file.readUTF());
            int size = file.readInt();
            byte[] bytes2 = new byte[size];
            file.readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key));
        }
    }
}
