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
package org.apache.beam.sdk.io.singlestore;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.singlestore.jdbc.SingleStoreDataSource;
import java.util.List;
import javax.sql.DataSource;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.singlestore.schematransform.SingleStoreSchemaTransformReadConfiguration;
import org.apache.beam.sdk.io.singlestore.schematransform.SingleStoreSchemaTransformReadProvider;
import org.apache.beam.sdk.io.singlestore.schematransform.SingleStoreSchemaTransformWriteConfiguration;
import org.apache.beam.sdk.io.singlestore.schematransform.SingleStoreSchemaTransformWriteProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SingleStoreIOSchemaTransformIT {

  private static final String DATABASE_NAME = "SingleStoreIOIT";

  private static int numberOfRows;

  private static String tableName;

  private static String serverName;

  private static String username;

  private static String password;

  private static Integer port;

  private static SingleStoreIO.DataSourceConfiguration dataSourceConfiguration;

  @BeforeClass
  public static void setup() {
    SingleStoreIOTestPipelineOptions options;
    try {
      options = readIOTestPipelineOptions(SingleStoreIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      options = null;
    }
    org.junit.Assume.assumeNotNull(options);

    numberOfRows = options.getNumberOfRecords();
    serverName = options.getSingleStoreServerName();
    username = options.getSingleStoreUsername();
    password = options.getSingleStorePassword();
    port = options.getSingleStorePort();
    tableName = DatabaseTestHelper.getTestTableName("IT");
    dataSourceConfiguration =
        SingleStoreIO.DataSourceConfiguration.create(serverName + ":" + port)
            .withDatabase(DATABASE_NAME)
            .withPassword(password)
            .withUsername(username);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenRead() throws Exception {
    TestHelper.createDatabaseIfNotExists(serverName, port, username, password, DATABASE_NAME);
    DataSource dataSource =
        new SingleStoreDataSource(
            String.format(
                "jdbc:singlestore://%s:%d/%s?user=%s&password=%s&allowLocalInfile=TRUE",
                serverName, port, DATABASE_NAME, username, password));
    DatabaseTestHelper.createTable(dataSource, tableName);
    try {
      PipelineResult writeResult = runWrite();
      assertEquals(PipelineResult.State.DONE, writeResult.waitUntilFinish());
      PipelineResult readResult = runRead();
      assertEquals(PipelineResult.State.DONE, readResult.waitUntilFinish());
      PipelineResult readResultWithPartitions = runReadWithPartitions();
      assertEquals(PipelineResult.State.DONE, readResultWithPartitions.waitUntilFinish());
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();
  @Rule public TestPipeline pipelineReadWithPartitions = TestPipeline.create();

  private PipelineResult runWrite() {
    SchemaTransformProvider provider = new SingleStoreSchemaTransformWriteProvider();

    SingleStoreSchemaTransformWriteConfiguration configuration =
        SingleStoreSchemaTransformWriteConfiguration.builder()
            .setDataSourceConfiguration(dataSourceConfiguration)
            .setTable(tableName)
            .setBatchSize(100)
            .build();

    Row configurationRow = configuration.toBeamRow();
    SchemaTransform schemaTransform = provider.from(configurationRow);

    Schema.Builder schemaBuilder = new Schema.Builder();
    schemaBuilder.addField("id", Schema.FieldType.INT32);
    schemaBuilder.addField("name", Schema.FieldType.STRING);
    Schema schema = schemaBuilder.build();

    PCollection<Row> rows =
        pipelineWrite
            .apply(GenerateSequence.from(0).to(numberOfRows))
            .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
            .apply(
                "Convert TestRows to Rows",
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via(
                        (SerializableFunction<TestRow, Row>)
                            testRow -> {
                              Row.Builder rowBuilder = Row.withSchema(schema);
                              rowBuilder.addValue(testRow.id());
                              rowBuilder.addValue(testRow.name());
                              return rowBuilder.build();
                            }))
            .setRowSchema(schema);

    PCollectionRowTuple input =
        PCollectionRowTuple.of(SingleStoreSchemaTransformWriteProvider.INPUT_TAG, rows);
    String tag = provider.outputCollectionNames().get(0);
    PCollectionRowTuple output = input.apply(schemaTransform);
    assertTrue(output.has(tag));
    PCollection<Integer> writtenRows =
        output
            .get(tag)
            .apply(
                "Convert Rows to Integers",
                MapElements.into(TypeDescriptor.of(Integer.class))
                    .via((SerializableFunction<Row, Integer>) row -> row.getInt32(0)));

    PAssert.thatSingleton(writtenRows.apply("Sum All", Sum.integersGlobally()))
        .isEqualTo(numberOfRows);

    return pipelineWrite.run();
  }

  private PipelineResult runRead() {
    SchemaTransformProvider provider = new SingleStoreSchemaTransformReadProvider();

    SingleStoreSchemaTransformReadConfiguration configuration =
        SingleStoreSchemaTransformReadConfiguration.builder()
            .setDataSourceConfiguration(dataSourceConfiguration)
            .setTable(tableName)
            .setOutputParallelization(true)
            .build();

    Row configurationRow = configuration.toBeamRow();
    SchemaTransform schemaTransform = provider.from(configurationRow);

    PCollectionRowTuple input = PCollectionRowTuple.empty(pipelineRead);
    String tag = provider.outputCollectionNames().get(0);
    PCollectionRowTuple output = input.apply(schemaTransform);
    assertTrue(output.has(tag));
    PCollection<TestRow> namesAndIds =
        output
            .get(tag)
            .apply(
                MapElements.into(TypeDescriptor.of(TestRow.class))
                    .via(
                        (SerializableFunction<Row, TestRow>)
                            row -> TestRow.create(row.getInt32(0), row.getString(1))));

    testReadResult(namesAndIds);

    return pipelineRead.run();
  }

  private PipelineResult runReadWithPartitions() {
    SchemaTransformProvider provider = new SingleStoreSchemaTransformReadProvider();

    SingleStoreSchemaTransformReadConfiguration configuration =
        SingleStoreSchemaTransformReadConfiguration.builder()
            .setDataSourceConfiguration(dataSourceConfiguration)
            .setTable(tableName)
            .setWithPartitions(true)
            .build();

    Row configurationRow = configuration.toBeamRow();
    SchemaTransform schemaTransform = provider.from(configurationRow);

    PCollectionRowTuple input = PCollectionRowTuple.empty(pipelineReadWithPartitions);
    String tag = provider.outputCollectionNames().get(0);
    PCollectionRowTuple output = input.apply(schemaTransform);
    assertTrue(output.has(tag));
    PCollection<TestRow> namesAndIds =
        output
            .get(tag)
            .apply(
                MapElements.into(TypeDescriptor.of(TestRow.class))
                    .via(
                        (SerializableFunction<Row, TestRow>)
                            row -> TestRow.create(row.getInt32(0), row.getString(1))));

    testReadResult(namesAndIds);

    return pipelineReadWithPartitions.run();
  }

  private void testReadResult(PCollection<TestRow> namesAndIds) {
    PAssert.thatSingleton(namesAndIds.apply("Count All", Count.globally()))
        .isEqualTo((long) numberOfRows);

    PCollection<String> consolidatedHashcode =
        namesAndIds
            .apply(ParDo.of(new TestRow.SelectNameFn()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(numberOfRows));

    PCollection<List<TestRow>> frontOfList = namesAndIds.apply(Top.smallest(500));
    Iterable<TestRow> expectedFrontOfList = TestRow.getExpectedValues(0, 500);
    PAssert.thatSingletonIterable(frontOfList).containsInAnyOrder(expectedFrontOfList);

    PCollection<List<TestRow>> backOfList = namesAndIds.apply(Top.largest(500));
    Iterable<TestRow> expectedBackOfList =
        TestRow.getExpectedValues(numberOfRows - 500, numberOfRows);
    PAssert.thatSingletonIterable(backOfList).containsInAnyOrder(expectedBackOfList);
  }
}
