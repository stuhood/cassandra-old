package org.apache.cassandra.db;
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

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnSerializer implements ICompactSerializer2<IColumn>
{
    /**
     * Writes a name of max length Short.MAX_VALUE, with a Short length prepended.
     * A null name is serialized with length -1.
     */
    public static void writeName(byte[] name, DataOutput out)
    {
        assert name == null || name.length <= Short.MAX_VALUE;
        try
        {
            if (name == null)
                out.writeShort(-1);
            else
            {
                out.writeShort((short)name.length);
                out.write(name);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static byte[] readName(DataInput in) throws IOException
    {
        short length = in.readShort();
        if (length == -1)
            return null;

        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    public void serialize(IColumn column, DataOutput dos)
    {
        ColumnSerializer.writeName(column.name(), dos);
        try
        {
            dos.writeBoolean(column.isMarkedForDelete());
            dos.writeLong(column.timestamp());
            FBUtilities.writeByteArray(column.value(), dos);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Column deserialize(DataInput dis) throws IOException
    {
        byte[] name = ColumnSerializer.readName(dis);
        boolean delete = dis.readBoolean();
        long ts = dis.readLong();
        int length = dis.readInt();
        if (length < 0)
        {
            throw new IOException("Corrupt (negative) value length encountered");
        }
        byte[] value = new byte[length];
        if (length > 0)
        {
            dis.readFully(value);
        }
        return new Column(name, value, ts, delete);
    }
}
