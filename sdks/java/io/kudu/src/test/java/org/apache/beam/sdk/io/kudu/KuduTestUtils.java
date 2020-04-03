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
package org.apache.beam.sdk.io.kudu;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

/** Utilities for Kudu tests. */
class KuduTestUtils {
  static final String COL_ID = "id";
  static final String COL_NAME = "name";

  static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              new ColumnSchema.ColumnSchemaBuilder(COL_ID, Type.INT64).key(true).build(),
              new ColumnSchema.ColumnSchemaBuilder(COL_NAME, Type.STRING)
                  .nullable(false)
                  .desiredBlockSize(4096)
                  .encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                  .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.NO_COMPRESSION)
                  .build()));

  static CreateTableOptions createTableOptions() {
    return new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of(COL_ID))
        .setNumReplicas(1);
  }

  /** Creates an Upsert Operation that matches the schema for each input. */
  static class GenerateUpsert implements KuduIO.FormatFunction<Long> {
    @Override
    public Operation apply(TableAndRecord<Long> input) {
      Upsert upsert = input.getTable().newUpsert();
      PartialRow row = upsert.getRow();
      row.addLong(COL_ID, input.getRecord());
      row.addString(COL_NAME, input.getRecord() + ": name");
      return upsert;
    }
  }

  /** Returns the count of rows for the given table. */
  static int rowCount(KuduTable table) throws KuduException {
    KuduScanner scanner = table.getAsyncClient().syncClient().newScannerBuilder(table).build();
    try {
      int rowCount = 0;
      while (scanner.hasMoreRows()) {
        rowCount += scanner.nextRows().getNumRows();
      }
      return rowCount;
    } finally {
      scanner.close();
    }
  }
}
