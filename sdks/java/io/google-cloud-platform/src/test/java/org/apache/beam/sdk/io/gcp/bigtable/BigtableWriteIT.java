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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;

import com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests of BigtableWrite. */
@RunWith(JUnit4.class)
public class BigtableWriteIT implements Serializable {
  /**
   * These tests requires a static instances because the writers go through a serialization step
   * when executing the test and would not affect passed-in objects otherwise.
   */
  private static final String COLUMN_FAMILY_NAME = "cf";

  private static BigtableTestOptions options;
  private static BigtableDataSettings veneerSettings;
  private BigtableConfig bigtableConfig;
  private static BigtableDataClient client;
  private static BigtableTableAdminClient tableAdminClient;
  private final String tableId =
      String.format("BigtableWriteIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date());

  private String project;

  @Before
  public void setup() throws Exception {
    PipelineOptionsFactory.register(BigtableTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigtableTestOptions.class);
    project = options.as(GcpOptions.class).getProject();

    bigtableConfig =
        BigtableConfig.builder()
            .setProjectId(ValueProvider.StaticValueProvider.of(project))
            .setInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .setUserAgent("apache-beam-test")
            .setValidate(true)
            .build();

    veneerSettings =
        BigtableConfigTranslator.translateWriteToVeneerSettings(
            bigtableConfig,
            BigtableWriteOptions.builder().build(),
            null,
            PipelineOptionsFactory.create());

    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(project)
            .setInstanceId(options.getInstanceId())
            .build();

    client = BigtableDataClient.create(veneerSettings);
    tableAdminClient = BigtableTableAdminClient.create(adminSettings);
  }

  @Test
  public void testE2EBigtableWrite() throws Exception {
    final int numRows = 1000;
    final List<KV<ByteString, ByteString>> testData = generateTableData(numRows);

    createEmptyTable(tableId);

    Pipeline p = Pipeline.create(options);
    p.apply(GenerateSequence.from(0).to(numRows))
        .apply(
            ParDo.of(
                new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    int index = c.element().intValue();

                    Iterable<Mutation> mutations =
                        ImmutableList.of(
                            Mutation.newBuilder()
                                .setSetCell(
                                    Mutation.SetCell.newBuilder()
                                        .setValue(testData.get(index).getValue())
                                        .setFamilyName(COLUMN_FAMILY_NAME))
                                .build());
                    c.output(KV.of(testData.get(index).getKey(), mutations));
                  }
                }))
        .apply(
            BigtableIO.write()
                .withProjectId(project)
                .withInstanceId(options.getInstanceId())
                .withTableId(tableId));
    PipelineResult r = p.run();

    // Test number of column families and column family name equality
    Table table = getTable(tableId);
    assertThat(table.getColumnFamilies(), Matchers.hasSize(1));
    assertThat(
        table.getColumnFamilies().stream().map((c) -> c.getId()).collect(Collectors.toList()),
        Matchers.contains(COLUMN_FAMILY_NAME));

    // Test table data equality
    List<KV<ByteString, ByteString>> tableData = getTableData(tableId);
    assertThat(tableData, Matchers.containsInAnyOrder(testData.toArray()));
    checkLineageSinkMetric(r, tableId);
  }

  @Test
  public void testE2EBigtableWriteWithInvalidColumnFamilyFailures() throws Exception {
    final int numRows = 1000;
    final List<KV<ByteString, ByteString>> testData = generateTableData(numRows);

    failureTest(
        numRows,
        new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            int index = c.element().intValue();

            String familyName = COLUMN_FAMILY_NAME;
            if (index % 600 == 0) {
              familyName = "malformed";
            }
            Iterable<Mutation> mutations =
                ImmutableList.of(
                    Mutation.newBuilder()
                        .setSetCell(
                            Mutation.SetCell.newBuilder()
                                .setValue(testData.get(index).getValue())
                                .setFamilyName(familyName))
                        .build());
            c.output(KV.of(testData.get(index).getKey(), mutations));
          }
        });
  }

  @Test
  public void testE2EBigtableWriteWithEmptyMutationFailures() throws Exception {
    final int numRows = 1000;
    final List<KV<ByteString, ByteString>> testData = generateTableData(numRows);
    failureTest(
        numRows,
        new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            int index = c.element().intValue();
            Iterable<Mutation> mutations;
            if (index % 600 == 0) {
              mutations =
                  ImmutableList.of(
                      Mutation.newBuilder().setSetCell(Mutation.SetCell.newBuilder()).build());
            } else {
              mutations =
                  ImmutableList.of(
                      Mutation.newBuilder()
                          .setSetCell(
                              Mutation.SetCell.newBuilder()
                                  .setValue(testData.get(index).getValue())
                                  .setFamilyName(COLUMN_FAMILY_NAME))
                          .build());
            }
            c.output(KV.of(testData.get(index).getKey(), mutations));
          }
        });
  }

  @Test
  public void testE2EBigtableWriteWithEmptyRowFailures() throws Exception {
    final int numRows = 1000;
    final List<KV<ByteString, ByteString>> testData = generateTableData(numRows);

    failureTest(
        numRows,
        new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            int index = c.element().intValue();
            Iterable<Mutation> mutations =
                ImmutableList.of(
                    Mutation.newBuilder()
                        .setSetCell(
                            Mutation.SetCell.newBuilder()
                                .setValue(testData.get(index).getValue())
                                .setFamilyName(COLUMN_FAMILY_NAME))
                        .build());

            ByteString rowKey = testData.get(index).getKey();
            if (index % 600 == 0) {
              rowKey = ByteString.empty();
            }
            c.output(KV.of(rowKey, mutations));
          }
        });
  }

  @Test
  public void testE2EBigtableWriteWithInvalidTimestampFailures() throws Exception {
    final int numRows = 1000;
    final List<KV<ByteString, ByteString>> testData = generateTableData(numRows);

    failureTest(
        numRows,
        new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            int index = c.element().intValue();
            Iterable<Mutation> mutations;
            if (index % 600 == 0) {
              mutations =
                  ImmutableList.of(
                      Mutation.newBuilder()
                          .setSetCell(
                              Mutation.SetCell.newBuilder()
                                  .setValue(testData.get(index).getValue())
                                  .setFamilyName(COLUMN_FAMILY_NAME)
                                  .setTimestampMicros(-2))
                          .build());
            } else {
              mutations =
                  ImmutableList.of(
                      Mutation.newBuilder()
                          .setSetCell(
                              Mutation.SetCell.newBuilder()
                                  .setValue(testData.get(index).getValue())
                                  .setFamilyName(COLUMN_FAMILY_NAME))
                          .build());
            }

            c.output(KV.of(testData.get(index).getKey(), mutations));
          }
        });
  }

  @Test
  public void testE2EBigtableWriteWithOversizedQualifierFailures() throws Exception {
    final int numRows = 1000;
    final List<KV<ByteString, ByteString>> testData = generateTableData(numRows);

    failureTest(
        numRows,
        new DoFn<Long, KV<ByteString, Iterable<Mutation>>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            int index = c.element().intValue();
            Iterable<Mutation> mutations;
            if (index % 600 == 0) {
              mutations =
                  ImmutableList.of(
                      Mutation.newBuilder()
                          .setSetCell(
                              Mutation.SetCell.newBuilder()
                                  .setValue(testData.get(index).getValue())
                                  .setFamilyName(COLUMN_FAMILY_NAME)
                                  .setColumnQualifier(ByteString.copyFrom(new byte[20_000])))
                          .build());
            } else {
              mutations =
                  ImmutableList.of(
                      Mutation.newBuilder()
                          .setSetCell(
                              Mutation.SetCell.newBuilder()
                                  .setValue(testData.get(index).getValue())
                                  .setFamilyName(COLUMN_FAMILY_NAME))
                          .build());
            }

            c.output(KV.of(testData.get(index).getKey(), mutations));
          }
        });
  }

  public void failureTest(int numRows, DoFn<Long, KV<ByteString, Iterable<Mutation>>> dataGenerator)
      throws Exception {

    createEmptyTable(tableId);

    Pipeline p = Pipeline.create(options);
    PCollection<KV<ByteString, Iterable<Mutation>>> mutations =
        p.apply(GenerateSequence.from(0).to(numRows)).apply(ParDo.of(dataGenerator));
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        p.registerBadRecordErrorHandler(new ErrorSinkTransform());
    mutations.apply(
        BigtableIO.write()
            .withProjectId(project)
            .withInstanceId(options.getInstanceId())
            .withTableId(tableId)
            .withErrorHandler(errorHandler));

    errorHandler.close();
    PAssert.thatSingleton(Objects.requireNonNull(errorHandler.getOutput())).isEqualTo(2L);

    PipelineResult r = p.run();

    // Test number of column families and column family name equality
    Table table = getTable(tableId);
    assertThat(table.getColumnFamilies(), Matchers.hasSize(1));
    assertThat(
        table.getColumnFamilies().stream().map((c) -> c.getId()).collect(Collectors.toList()),
        Matchers.contains(COLUMN_FAMILY_NAME));

    // Test table data equality
    List<KV<ByteString, ByteString>> tableData = getTableData(tableId);
    assertEquals(998, tableData.size());
    checkLineageSinkMetric(r, tableId);
  }

  @After
  public void tearDown() throws Exception {
    deleteTable(tableId);
    if (tableAdminClient != null) {
      tableAdminClient.close();
    }
    if (client != null) {
      client.close();
    }
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
  private void createEmptyTable(String tableId) {
    tableAdminClient.createTable(CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY_NAME));
  }

  /** Helper function to get a table. */
  private Table getTable(String tableId) {
    return tableAdminClient.getTable(tableId);
  }

  /** Helper function to get a table's data. */
  private List<KV<ByteString, ByteString>> getTableData(String tableId) {
    ServerStream<Row> rows = client.readRows(Query.create(tableId));

    Iterator<Row> iterator = rows.iterator();

    Row currentRow;
    List<KV<ByteString, ByteString>> tableData = new ArrayList<>();
    while (iterator.hasNext()) {
      currentRow = iterator.next();
      ByteString key = currentRow.getKey();
      ByteString value = currentRow.getCells(COLUMN_FAMILY_NAME).get(0).getValue();
      tableData.add(KV.of(key, value));
    }

    return tableData;
  }

  /** Helper function to delete a table. */
  private void deleteTable(String tableId) {
    if (tableAdminClient != null) {
      tableAdminClient.deleteTable(tableId);
    }
  }

  private void checkLineageSinkMetric(PipelineResult r, String tableId) {
    // Only check lineage metrics on direct runner until Dataflow runner v2 supported report back
    if (options.getRunner().getName().contains("DirectRunner")) {
      assertThat(
          Lineage.query(r.metrics(), Lineage.Type.SINK),
          hasItem(
              Lineage.getFqName(
                  "bigtable", ImmutableList.of(project, options.getInstanceId(), tableId))));
    }
  }
}
