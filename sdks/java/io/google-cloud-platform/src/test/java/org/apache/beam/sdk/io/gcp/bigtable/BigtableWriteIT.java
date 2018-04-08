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

import static org.junit.Assert.assertThat;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableOptions.Builder;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests of BigtableWrite.
 */
@RunWith(JUnit4.class)
public class BigtableWriteIT implements Serializable {
  /**
   * These tests requires a static instances because the writers go through a serialization step
   * when executing the test and would not affect passed-in objects otherwise.
   */
  private static final String COLUMN_FAMILY_NAME = "cf";
  private static BigtableTestOptions options;
  private BigtableOptions bigtableOptions;
  private static BigtableSession session;
  private static BigtableTableAdminClient tableAdminClient;
  private final String tableId =
      String.format("BigtableWriteIT-%tF-%<tH-%<tM-%<tS-%<tL", DateTime.now());
  private String project;

  @Before
  public void setup() throws Exception {
    PipelineOptionsFactory.register(BigtableTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigtableTestOptions.class);
    project = options.as(GcpOptions.class).getProject();

    bigtableOptions =
        new Builder()
            .setProjectId(project)
            .setInstanceId(options.getInstanceId())
            .setUserAgent("apache-beam-test")
            .build();

    session =
        new BigtableSession(
            bigtableOptions
                .toBuilder()
                .setCredentialOptions(
                    CredentialOptions.credential(options.as(GcpOptions.class).getGcpCredential()))
                .build());
    tableAdminClient = session.getTableAdminClient();
  }

  @Test
  public void testE2EBigtableWrite() throws Exception {
    final String tableName = bigtableOptions.getInstanceName().toTableNameStr(tableId);
    final String instanceName = bigtableOptions.getInstanceName().toString();
    final int numRows = 1000;
    final List<KV<ByteString, ByteString>> testData = generateTableData(numRows);

    createEmptyTable(instanceName, tableId);

    Pipeline p = Pipeline.create(options);
    p.apply(GenerateSequence.from(0).to(numRows))
        .apply(ParDo.of(new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            int index = c.element().intValue();

            Iterable<Mutation> mutations =
                ImmutableList.of(Mutation.newBuilder()
                    .setSetCell(
                        Mutation.SetCell.newBuilder()
                            .setValue(testData.get(index).getValue())
                            .setFamilyName(COLUMN_FAMILY_NAME))
                    .build());
            c.output(KV.of(testData.get(index).getKey(), mutations));
          }
        }))
        .apply(BigtableIO.write()
          .withBigtableOptions(bigtableOptions)
          .withTableId(tableId));
    p.run();

    // Test number of column families and column family name equality
    Table table = getTable(tableName);
    assertThat(table.getColumnFamiliesMap().keySet(), Matchers.hasSize(1));
    assertThat(table.getColumnFamiliesMap(), Matchers.hasKey(COLUMN_FAMILY_NAME));

    // Test table data equality
    List<KV<ByteString, ByteString>> tableData = getTableData(tableName);
    assertThat(tableData, Matchers.containsInAnyOrder(testData.toArray()));
  }

  @After
  public void tearDown() throws Exception {
    final String tableName = bigtableOptions.getInstanceName().toTableNameStr(tableId);
    deleteTable(tableName);
    session.close();
  }

  ////////////////////////////////////////////////////////////////////////////////////////////
  /** Helper function to generate KV test data. */
  private List<KV<ByteString, ByteString>> generateTableData(int numRows) {
    List<KV<ByteString, ByteString>> testData = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; ++i) {
      ByteString key = ByteString.copyFromUtf8(String.format("key%09d", i));
      ByteString value = ByteString.copyFromUtf8(String.format("value%09d", i));
      testData.add(KV.of(key, value));
    }

    return testData;
  }

  /** Helper function to create an empty table. */
  private void createEmptyTable(String instanceName, String tableId) {
    Table.Builder tableBuilder = Table.newBuilder();
    tableBuilder.putColumnFamilies(COLUMN_FAMILY_NAME, ColumnFamily.newBuilder().build());

    CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.newBuilder()
        .setParent(instanceName)
        .setTableId(tableId)
        .setTable(tableBuilder.build());
    tableAdminClient.createTable(createTableRequestBuilder.build());
  }

  /** Helper function to get a table. */
  private Table getTable(String tableName) {
    GetTableRequest.Builder getTableRequestBuilder = GetTableRequest.newBuilder()
        .setName(tableName);
    return tableAdminClient.getTable(getTableRequestBuilder.build());
  }

  /** Helper function to get a table's data. */
  private List<KV<ByteString, ByteString>> getTableData(String tableName) throws IOException {
    // Add empty range to avoid TARGET_NOT_SET error
    RowRange range = RowRange.newBuilder()
        .setStartKeyClosed(ByteString.EMPTY)
        .setEndKeyOpen(ByteString.EMPTY)
        .build();
    RowSet rowSet = RowSet.newBuilder()
        .addRowRanges(range)
        .build();
    ReadRowsRequest.Builder readRowsRequestBuilder = ReadRowsRequest.newBuilder()
        .setTableName(tableName)
        .setRows(rowSet);
    ResultScanner<Row> scanner = session.getDataClient().readRows(readRowsRequestBuilder.build());

    Row currentRow;
    List<KV<ByteString, ByteString>> tableData = new ArrayList<>();
    while ((currentRow = scanner.next()) != null) {
      ByteString key = currentRow.getKey();
      ByteString value = currentRow.getFamilies(0).getColumns(0).getCells(0).getValue();
      tableData.add(KV.of(key, value));
    }
    scanner.close();

    return tableData;
  }

  /** Helper function to delete a table. */
  private void deleteTable(String tableName) {
    DeleteTableRequest.Builder deleteTableRequestBuilder = DeleteTableRequest.newBuilder()
        .setName(tableName);
    tableAdminClient.deleteTable(deleteTableRequestBuilder.build());
  }
}
