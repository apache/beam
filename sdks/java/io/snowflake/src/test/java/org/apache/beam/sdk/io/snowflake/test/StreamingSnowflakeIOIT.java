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

import static org.apache.beam.sdk.io.snowflake.test.TestUtils.SnowflakeIOITPipelineOptions;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getTestRowDataMapper;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.enums.StreamingLogLevel;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * The test for streaming writing to SnowflakeIO. Because of using {@code TestStream} it has to be
 * run with DirectRunner. The test requires a Snowflake's snowpipe name with the copy into
 * STREAMING_IOIT table statement.
 *
 * <p>Example run:
 *
 * <pre>
 * ./gradlew --info -p sdks/java/io/snowflake integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--privateKeyPath=<PATH TO PRIVATE KEY>",
 * "--privateKeyPassphrase=<KEY PASSPHRASE>",
 * "--database=<DATABASE NAME>",
 * "--schema=<SCHEMA NAME>",
 * "--stagingBucketName=<BUCKET NAME>",
 * "--storageIntegrationName=<STORAGE INTEGRATION NAME>",
 * "--snowPipe=<SNOWPIPE NAME>",
 * "--numberOfRecords=<INTEGER>",
 * "--runner=DirectRunner"]'
 * --tests org.apache.beam.sdk.io.snowflake.test.SnowflakeStreamingIOIT.writeStreamThenRead
 * -DintegrationTestRunner=direct
 * </pre>
 */
public class StreamingSnowflakeIOIT {
  private static final int TIMEOUT = 900000;
  private static final int INTERVAL = 30000;
  private static final String TABLE = "STREAMING_IOIT";

  private static final List<TestRow> testRows = newArrayList();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static SnowflakeIOITPipelineOptions options;
  private static SnowflakeIO.DataSourceConfiguration dc;
  private static String stagingBucketName;
  private static String storageIntegrationName;

  @BeforeClass
  public static void setupAll() throws SQLException {
    PipelineOptionsFactory.register(SnowflakeIOITPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SnowflakeIOITPipelineOptions.class);

    dc =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withRole(options.getRole())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());

    stagingBucketName = options.getStagingBucketName();
    storageIntegrationName = options.getStorageIntegrationName();

    for (int i = 0; i < options.getNumberOfRecords(); i++) {
      testRows.add(TestRow.create(i, String.format("TestRow%s:%s", i, UUID.randomUUID())));
    }

    TestUtils.runConnectionWithStatement(
        dc.buildDatasource(),
        String.format("CREATE OR REPLACE TABLE %s(id INTEGER, name STRING)", TABLE));
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    String bucketNameAndPath = stagingBucketName.replaceAll(".+//", "");
    String[] parts = bucketNameAndPath.split("/", -1);
    String bucketName = parts[0];
    String directory = null;
    if (parts.length > 1) {
      directory = bucketNameAndPath.replace(bucketName + "/", "");
    }
    TestUtils.clearStagingBucket(bucketName, directory);

    TestUtils.runConnectionWithStatement(
        dc.buildDatasource(), String.format("DROP TABLE %s", TABLE));
  }

  @Test
  public void writeStreamThenRead() throws SQLException, InterruptedException {
    writeStreamToSnowflake();
    readStreamFromSnowflakeAndVerify();
  }

  private void writeStreamToSnowflake() {
    TestStream<TestRow> stringsStream =
        TestStream.create(SerializableCoder.of(TestRow.class))
            .advanceWatermarkTo(Instant.now())
            .addElements(
                testRows.get(0), testRows.subList(1, testRows.size()).toArray(new TestRow[0]))
            .advanceWatermarkToInfinity();

    pipeline
        .apply(stringsStream)
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<TestRow>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(getTestRowDataMapper())
                .withSnowPipe(options.getSnowPipe())
                .withStorageIntegrationName(storageIntegrationName)
                .withStagingBucketName(stagingBucketName)
                .withFlushTimeLimit(Duration.millis(18000))
                .withFlushRowLimit(50000)
                .withDebugMode(StreamingLogLevel.ERROR));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  private void readStreamFromSnowflakeAndVerify() throws SQLException, InterruptedException {
    int timeout = TIMEOUT;
    while (timeout > 0) {
      Set<TestRow> fetchedRows = readDataFromStream();
      if (fetchedRows.size() >= testRows.size()) {
        assertThat(fetchedRows, containsInAnyOrder(testRows.toArray(new TestRow[0])));
        return;
      }
      Thread.sleep(INTERVAL);
      timeout -= INTERVAL;
    }
    throw new RuntimeException("Could not read data from table");
  }

  private Set<TestRow> readDataFromStream() throws SQLException {
    Connection connection = dc.buildDatasource().getConnection();
    PreparedStatement statement =
        connection.prepareStatement(String.format("SELECT * FROM %s", TABLE));
    ResultSet resultSet = statement.executeQuery();

    Set<TestRow> testRows = resultSetToJavaSet(resultSet);

    resultSet.close();
    statement.close();
    connection.close();

    return testRows;
  }

  private Set<TestRow> resultSetToJavaSet(ResultSet resultSet) throws SQLException {
    Set<TestRow> testRows = newHashSet();
    while (resultSet.next()) {
      testRows.add(TestRow.create(resultSet.getInt(1), resultSet.getString(2).replace("'", "")));
    }
    return testRows;
  }
}
