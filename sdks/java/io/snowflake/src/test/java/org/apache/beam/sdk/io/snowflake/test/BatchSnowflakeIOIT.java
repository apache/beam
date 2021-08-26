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
package org.apache.beam.sdk.io.snowflake.test;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.SnowflakeIOITPipelineOptions;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getTestRowCsvMapper;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getTestRowDataMapper;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * A test of {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO} on an independent Snowflake
 * instance.
 *
 * <p>This test requires a running instance of Snowflake, configured for your GCP account. Pass in
 * connection information using PipelineOptions:
 *
 * <pre>
 * ./gradlew -p sdks/java/io/snowflake integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--password=<PASSWORD>",
 * "--database=<DATABASE NAME>",
 * "--role=<SNOWFLAKE ROLE>",
 * "--warehouse=<SNOWFLAKE WAREHOUSE NAME>",
 * "--schema=<SCHEMA NAME>",
 * "--stagingBucketName=gs://<GCS BUCKET NAME>",
 * "--storageIntegrationName=<STORAGE INTEGRATION NAME>",
 * "--numberOfRecords=<1000, 100000, 600000, 5000000>",
 * "--runner=DataflowRunner",
 * "--region=<GCP REGION FOR DATAFLOW RUNNER>",
 * "--project=<GCP_PROJECT>"]'
 * --tests org.apache.beam.sdk.io.snowflake.test.BatchSnowflakeIOIT
 * -DintegrationTestRunner=dataflow
 * </pre>
 */
public class BatchSnowflakeIOIT {
  private static final String tableName = "IOIT";

  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static int numberOfRecords;
  private static String stagingBucketName;
  private static String storageIntegrationName;

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() throws SQLException {
    SnowflakeIOITPipelineOptions options =
        readIOTestPipelineOptions(SnowflakeIOITPipelineOptions.class);

    numberOfRecords = options.getNumberOfRecords();
    stagingBucketName = options.getStagingBucketName();
    storageIntegrationName = options.getStorageIntegrationName();

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create()
            .withUsernamePasswordAuth(options.getUsername(), options.getPassword())
            .withDatabase(options.getDatabase())
            .withRole(options.getRole())
            .withWarehouse(options.getWarehouse())
            .withServerName(options.getServerName())
            .withSchema(options.getSchema());
  }

  @Test
  public void testWriteThenRead() {
    PipelineResult writeResult = runWrite();
    writeResult.waitUntilFinish();

    PipelineResult readResult = runRead();
    readResult.waitUntilFinish();
  }

  @AfterClass
  public static void teardown() throws Exception {
    String combinedPath = stagingBucketName + "/**";
    List<ResourceId> paths =
        FileSystems.match(combinedPath).metadata().stream()
            .map(MatchResult.Metadata::resourceId)
            .collect(Collectors.toList());

    FileSystems.delete(paths, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

    TestUtils.runConnectionWithStatement(
        dataSourceConfiguration.buildDatasource(), String.format("DROP TABLE %s", tableName));
  }

  private PipelineResult runWrite() {

    pipelineWrite
        .apply(GenerateSequence.from(0).to(numberOfRecords))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(
            SnowflakeIO.<TestRow>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withWriteDisposition(WriteDisposition.TRUNCATE)
                .withUserDataMapper(getTestRowDataMapper())
                .to(tableName)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withTableSchema(
                    SnowflakeTableSchema.of(
                        SnowflakeColumn.of("id", SnowflakeInteger.of()),
                        SnowflakeColumn.of("name", SnowflakeString.of()))));

    return pipelineWrite.run();
  }

  private PipelineResult runRead() {
    PCollection<TestRow> namesAndIds =
        pipelineRead.apply(
            SnowflakeIO.<TestRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(tableName)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withCsvMapper(getTestRowCsvMapper())
                .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(namesAndIds.apply("Count All", Count.globally()))
        .isEqualTo((long) numberOfRecords);

    PCollection<String> consolidatedHashcode =
        namesAndIds
            .apply(ParDo.of(new TestRow.SelectNameFn()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(numberOfRecords));

    return pipelineRead.run();
  }
}
