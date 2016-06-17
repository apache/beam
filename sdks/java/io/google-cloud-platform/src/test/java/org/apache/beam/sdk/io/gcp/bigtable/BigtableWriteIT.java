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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.bigtable.admin.table.v1.CreateTableRequest;
import com.google.bigtable.admin.table.v1.DeleteTableRequest;
import com.google.bigtable.admin.table.v1.GetTableRequest;
import com.google.bigtable.admin.table.v1.Table;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Random;

/**
 * End-to-end tests of BigtableWrite.
 */
@RunWith(JUnit4.class)
public class BigtableWriteIT implements Serializable {

  private final Random random = new Random();

  @Test
  public void testE2EBigtableWrite() throws Exception {
    PipelineOptionsFactory.register(BigtableTestOptions.class);
    BigtableTestOptions options = TestPipeline.testingPipelineOptions()
        .as(BigtableTestOptions.class);

    BigtableOptions.Builder bigtableOptionsBuilder = new BigtableOptions.Builder()
        .setProjectId(options.getProjectId())
        .setClusterId(options.getClusterId())
        .setZoneId(options.getZoneId())
        .setUserAgent("apache-beam-test");
    BigtableOptions bigtableOptions = bigtableOptionsBuilder.build();

    String tableId = String.format("BigtableWriteIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date());
    String tableName = bigtableOptions.getClusterName().toTableNameStr(tableId);

    final int numRows = 1000;
    final int valueLength = 10;
    final String[] values = new String[numRows];

    for (int i = 0; i < numRows; ++i) {
      values[i] = createRandomString(valueLength);
    }

    BigtableSession bigtableSession = new BigtableSession(bigtableOptions);
    BigtableTableAdminClient tableAdminClient = bigtableSession.getTableAdminClient();
    Table.Builder tableBuilder = Table.newBuilder();
    final String columnName = "cf";
    Map<String, ColumnFamily> columnFamilies = tableBuilder.getMutableColumnFamilies();
    columnFamilies.put(columnName, ColumnFamily.newBuilder().build());

    CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.newBuilder()
        .setName(bigtableOptions.getClusterName().toString())
        .setTableId(tableId)
        .setTable(tableBuilder.build());
    tableAdminClient.createTable(createTableRequestBuilder.build());

    Pipeline p = Pipeline.create(options);
    p.apply(CountingInput.upTo(numRows))
        .apply(ParDo.of(new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
          @Override
          public void processElement(ProcessContext c) {
            Iterable<Mutation> mutations =
                ImmutableList.of(Mutation.newBuilder()
                    .setSetCell(
                        Mutation.SetCell.newBuilder()
                            .setValue(ByteString.copyFromUtf8(values[c.element().intValue()]))
                            .setFamilyName(columnName))
                    .build());
            c.output(KV.of(ByteString.copyFromUtf8(c.element().toString()), mutations));
          }
        }))
        .apply(BigtableIO.write()
          .withBigtableOptions(bigtableOptionsBuilder)
          .withTableId(tableId));
    p.run();

    GetTableRequest.Builder getTableRequestBuilder = GetTableRequest.newBuilder()
        .setName(tableName);
    Table table = tableAdminClient.getTable(getTableRequestBuilder.build());
    assertEquals(table.getColumnFamilies().size(), 1);
    assertTrue(table.getColumnFamilies().containsKey(columnName));

    ReadRowsRequest.Builder readRowsRequestBuilder = ReadRowsRequest.newBuilder()
        .setTableName(tableName);
    ResultScanner<Row> scanner = bigtableSession.getDataClient()
        .readRows(readRowsRequestBuilder.build());

    int readCount = 0;
    Row currentRow;
    while ((currentRow = scanner.next()) != null) {
      int currentKey = Integer.parseInt(currentRow.getKey().toStringUtf8());
      String currentValue =
          currentRow.getFamilies(0).getColumns(0).getCells(0).getValue().toStringUtf8();
      assertEquals(values[currentKey], currentValue);
      ++readCount;
    }
    assertEquals(numRows, readCount);

    DeleteTableRequest.Builder deleteTableRequestBuilder = DeleteTableRequest.newBuilder()
        .setName(tableName);
    tableAdminClient.deleteTable(deleteTableRequestBuilder.build());
  }

  private String createRandomString(int length) {
    char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      builder.append(chars[random.nextInt(chars.length)]);
    }
    return builder.toString();
  }
}
