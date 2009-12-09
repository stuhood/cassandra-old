/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.concurrent.StageManager;

public class RowMutation implements Serializable
{
    private static ICompactSerializer<RowMutation> serializer_;
    public static final String HINT = "HINT";

    static
    {
        serializer_ = new RowMutationSerializer();
    }   

    static ICompactSerializer<RowMutation> serializer()
    {
        return serializer_;
    }

    private String table_;
    private String key_;
    protected Map<String, AColumnFamily> modifications_ = new HashMap<String, AColumnFamily>();

    public RowMutation(String table, String key)
    {
        table_ = table;
        key_ = key;
    }

    public RowMutation(String table, Row row)
    {
        table_ = table;
        key_ = row.key;
        add(row.cf);
    }

    protected RowMutation(String table, String key, Map<String, AColumnFamily> modifications)
    {
        table_ = table;
        key_ = key;
        modifications_ = modifications;
    }

    public String getTable()
    {
        return table_;
    }

    public String key()
    {
        return key_;
    }

    public Set<String> columnFamilyNames()
    {
        return modifications_.keySet();
    }
    
    public Collection<AColumnFamily> getColumnFamilies()
    {
        return modifications_.values();
    }

    void addHints(String key, byte[] host) throws IOException
    {
        QueryPath path = new QueryPath(HintedHandOffManager.HINTS_CF, key.getBytes("UTF-8"), host);
        add(path, ArrayUtils.EMPTY_BYTE_ARRAY, System.currentTimeMillis());
    }

    /*
     * Specify a column family object.
     * param @ columnFamily - the column family.
    */
    public void add(AColumnFamily columnFamily)
    {
        assert columnFamily != null;
        if (modifications_.containsKey(columnFamily.name))
        {
            throw new IllegalArgumentException("ColumnFamily " + columnFamily.name + " is already being modified");
        }
        modifications_.put(columnFamily.name, columnFamily);
    }

    /** should only be called by commitlog replay code */
    public void removeColumnFamily(AColumnFamily columnFamily)
    {
        modifications_.remove(columnFamily.name);
    }
    
    public boolean isEmpty()
    {
        return modifications_.isEmpty();
    }

    /*
     * Specify a column name and a corresponding value for
     * the column. Column name is specified as <column family>:column.
     * This will result in a ColumnFamily associated with
     * <column family> as name and a Column with <column>
     * as name. The column can be further broken up
     * as super column name : columnname  in case of super columns
     *
     * param @ path - column name as <column family>:<column>
     * param @ value - value associated with the column
     * param @ timestamp - timestamp associated with this data.
    */
    public void add(QueryPath path, byte[] value, long timestamp)
    {
        AColumnFamily columnFamily = modifications_.get(path.columnFamilyName);
        ColumnFamily cf;
        if (columnFamily == null)
            cf = ColumnFamily.create(table_, path.columnFamilyName);
        else
            cf = columnFamily.asMutable();
        cf.addColumn(path, value, timestamp);
        modifications_.put(path.columnFamilyName, cf);
    }

    public void delete(QueryPath path, long timestamp)
    {
        assert path.columnFamilyName != null;
        String cfName = path.columnFamilyName;

        if (modifications_.containsKey(cfName))
        {
            throw new IllegalArgumentException("ColumnFamily " + cfName + " is already being modified");
        }

        int localDeleteTime = (int) (System.currentTimeMillis() / 1000);

        AColumnFamily columnFamily = modifications_.get(cfName);
        ColumnFamily cf = null;
        if (columnFamily == null)
            cf = ColumnFamily.create(table_, cfName);
        else
            cf = columnFamily.asMutable();

        if (path.superColumnName == null && path.columnName == null)
        {
            cf.delete(localDeleteTime, timestamp);
        }
        else if (path.columnName == null)
        {
            SuperColumn sc = new SuperColumn(path.superColumnName, DatabaseDescriptor.getSubComparator(table_, cfName));
            sc.markForDeleteAt(localDeleteTime, timestamp);
            cf.addColumn(sc);
        }
        else
        {
            ByteBuffer bytes = ByteBuffer.allocate(4);
            bytes.putInt(localDeleteTime);
            cf.addColumn(path, bytes.array(), timestamp, true);
        }

        modifications_.put(cfName, cf);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    public void apply() throws IOException
    {
        apply(true);
    }

    public void apply(boolean writeCommitLog) throws IOException
    {
        Table.open(table_).apply(this, this.getSerializedBuffer(), writeCommitLog);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    void applyBinary() throws IOException, ExecutionException, InterruptedException
    {
        Table.open(table_).load(this);
    }

    public Message makeRowMutationMessage() throws IOException
    {
        return makeRowMutationMessage(StorageService.mutationVerbHandler_);
    }

    public Message makeRowMutationMessage(String verbHandlerName) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        serializer().serialize(this, dos);
        return new Message(FBUtilities.getLocalAddress(), StageManager.mutationStage_, verbHandlerName, bos.toByteArray());
    }

    public static RowMutation getRowMutation(String table, String key, Map<String, List<ColumnOrSuperColumn>> cfmap)
    {
        RowMutation rm = new RowMutation(table, key.trim());
        for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : cfmap.entrySet())
        {
            String cfName = entry.getKey();
            for (ColumnOrSuperColumn cosc : entry.getValue())
            {
                if (cosc.column == null)
                {
                    assert cosc.super_column != null;
                    for (org.apache.cassandra.service.Column column : cosc.super_column.columns)
                    {
                        rm.add(new QueryPath(cfName, cosc.super_column.name, column.name), column.value, column.timestamp);
                    }
                }
                else
                {
                    assert cosc.super_column == null;
                    rm.add(new QueryPath(cfName, null, cosc.column.name), cosc.column.value, cosc.column.timestamp);
                }
            }
        }
        return rm;
    }
    
    public DataOutputBuffer getSerializedBuffer() throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        RowMutation.serializer().serialize(this, buffer);
        return buffer;
    }

    public String toString()
    {
        return "RowMutation(" +
               "table='" + table_ + '\'' +
               ", key='" + key_ + '\'' +
               ", modifications=[" + StringUtils.join(modifications_.values(), ", ") + "]" +
               ')';
    }
}

class RowMutationSerializer implements ICompactSerializer<RowMutation>
{
    private void freezeTheMaps(Map<String, AColumnFamily> map, DataOutputStream dos) throws IOException
    {
        int size = map.size();
        dos.writeInt(size);
        if (size > 0)
        {
            Set<String> keys = map.keySet();
            for (String key : keys)
            {
                dos.writeUTF(key);
                AColumnFamily cf = map.get(key);
                if (cf != null)
                {
                    AColumnFamily.serializer().serializeForRPC(cf, dos);
                }
            }
        }
    }

    public void serialize(RowMutation rm, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(rm.getTable());
        dos.writeUTF(rm.key());

        /* serialize the modifications_ in the mutation */
        freezeTheMaps(rm.modifications_, dos);
    }

    private Map<String, AColumnFamily> defreezeTheMaps(DataInputStream dis) throws IOException
    {
        Map<String, AColumnFamily> map = new HashMap<String, AColumnFamily>();
        int size = dis.readInt();
        for (int i = 0; i < size; ++i)
        {
            String key = dis.readUTF();
            AColumnFamily cf = AColumnFamily.serializer().deserializeFromRPC(dis);
            map.put(key, cf);
        }
        return map;
    }

    public RowMutation deserialize(DataInputStream dis) throws IOException
    {
        String table = dis.readUTF();
        String key = dis.readUTF();
        Map<String, AColumnFamily> modifications = defreezeTheMaps(dis);
        return new RowMutation(table, key, modifications);
    }
}
