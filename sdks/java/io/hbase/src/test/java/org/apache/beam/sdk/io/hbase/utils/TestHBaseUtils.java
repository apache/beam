/*
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
package org.apache.beam.sdk.io.hbase.utils;

import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colFamily;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colFamily2;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/** <b>Internal only:</b> Hbase-related convenience functions. */
public class TestHBaseUtils {

  public static byte[] getCell(Table table, byte[] rowKey, byte[] colFamily, byte[] colQualifier)
      throws IOException {
    return getRowResult(table, rowKey).getValue(colFamily, colQualifier);
  }

  public static Result getRowResult(Table table, byte[] rowKey) throws IOException {
    return table.get(new Get(rowKey));
  }

  public static Table createTable(HBaseTestingUtility htu) throws IOException {
    return createTable(htu, UUID.randomUUID().toString());
  }

  public static Table createTable(HBaseTestingUtility htu, String name) throws IOException {
    TableName tableName = TableName.valueOf(name);
    return htu.createTable(
        tableName, new String[] {Bytes.toString(colFamily), Bytes.toString(colFamily2)});
  }

  /** Builder class for Hbase mutations. */
  public static class HBaseMutationBuilder {

    public static Put createPut(
        byte[] rowKey, byte[] colFamily, byte[] colQualifier, byte[] value, long atTimestamp) {
      return new Put(rowKey, atTimestamp).addColumn(colFamily, colQualifier, value);
    }

    public static Delete createDelete(
        byte[] rowKey, byte[] colFamily, byte[] colQualifier, long atTimestamp) {
      return new Delete(rowKey, atTimestamp).addColumns(colFamily, colQualifier);
    }

    public static Delete createDeleteFamily(byte[] rowKey, byte[] colFamily, long atTimestamp) {
      return new Delete(rowKey, atTimestamp).addFamily(colFamily);
    }
  }
}
