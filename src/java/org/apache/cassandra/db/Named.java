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

/**
 * For easy comparison of items with byte[] names (ColumnKeys and Columns).
 */
public interface Named
{
    public byte[] name();

    /**
     * Comparator for Column names using a backing ColumnKey.Comparator.
     */
    static final class Comparator implements java.util.Comparator<Named>
    {
        public final ColumnKey.Comparator ckcomp;
        public Comparator(ColumnKey.Comparator ckcomp)
        {
            this.ckcomp = ckcomp;
        }
        
        public int compare(Named n1, Named n2)
        {
            return ckcomp.compareAt(n1.name(), n2.name(), ckcomp.columnDepth());
        }
    }
}
