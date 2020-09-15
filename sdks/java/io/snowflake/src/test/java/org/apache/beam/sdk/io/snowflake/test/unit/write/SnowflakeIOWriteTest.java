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
package org.apache.beam.sdk.io.snowflake.test.unit.write;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBatchServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.TestSnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeIOWriteTest {
  private static final String FAKE_TABLE = "FAKE_TABLE";
  private static final String BUCKET_NAME = "BUCKET/";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private static TestSnowflakePipelineOptions options;
  private static SnowflakeIO.DataSourceConfiguration dc;

  private static SnowflakeService snowflakeService;
  private static List<Long> testData;
  private static List<String> testDataInStrings;

  @BeforeClass
  public static void setupAll() {
    snowflakeService = new FakeSnowflakeBatchServiceImpl();
    testData = LongStream.range(0, 100).boxed().collect(Collectors.toList());

    testDataInStrings = new ArrayList<>();
    testDataInStrings.add("First row");
    testDataInStrings.add("Second row with 'single' quotation");
    testDataInStrings.add("Second row with single one ' quotation");
    testDataInStrings.add("Second row with single twice '' quotation");
    testDataInStrings.add("Third row with \"double\" quotation");
    testDataInStrings.add("Third row with double one \" quotation");
    testDataInStrings.add("Third row with double twice \"\" quotation");
  }

  @Before
  public void setup() {
    FakeSnowflakeDatabase.createTable(FAKE_TABLE);

    PipelineOptionsFactory.register(TestSnowflakePipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TestSnowflakePipelineOptions.class);
    options.setStagingBucketName(BUCKET_NAME);
    options.setStorageIntegrationName("STORAGE_INTEGRATION");
    options.setServerName("NULL.snowflakecomputing.com");

    dc =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());
  }

  @After
  public void tearDown() {
    TestUtils.removeTempDir(BUCKET_NAME);
  }

  @Test
  public void writeWithIntegrationTest() throws SnowflakeSQLException {
    pipeline
        .apply(Create.of(testData))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .to(FAKE_TABLE)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.areListsEqual(testData, actualData));
  }

  @Test
  public void writeWithMapperTest() throws SnowflakeSQLException {
    pipeline
        .apply(Create.of(testData))
        .apply(
            "text write IO",
            SnowflakeIO.<Long>write()
                .to(FAKE_TABLE)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.areListsEqual(testData, actualData));
  }

  @Test
  public void writeWithKVInputTest() throws SnowflakeSQLException {
    pipeline
        .apply(Create.of(testData))
        .apply(ParDo.of(new TestUtils.ParseToKv()))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<KV<String, Long>>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(TestUtils.getLongCsvMapperKV())
                .to(FAKE_TABLE)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<String> actualData = FakeSnowflakeDatabase.getElements(FAKE_TABLE);
    List<String> testDataInStrings =
        testData.stream().map(Object::toString).collect(Collectors.toList());
    assertTrue(TestUtils.areListsEqual(testDataInStrings, actualData));
  }

  @Test
  public void writeWithTransformationTest() throws SQLException {
    String query = "select t.$1 from %s t";
    pipeline
        .apply(Create.of(testData))
        .apply(ParDo.of(new TestUtils.ParseToKv()))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<KV<String, Long>>write()
                .to(FAKE_TABLE)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withUserDataMapper(TestUtils.getLongCsvMapperKV())
                .withDataSourceConfiguration(dc)
                .withQueryTransformation(query)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.areListsEqual(testData, actualData));
  }

  @Test
  public void writeToExternalWithDoubleQuotation() throws SnowflakeSQLException {

    pipeline
        .apply(Create.of(testDataInStrings))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(TestUtils.getStringCsvMapper())
                .to(FAKE_TABLE)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withSnowflakeService(snowflakeService)
                .withQuotationMark("\""));

    pipeline.run(options).waitUntilFinish();

    List<String> actualData = FakeSnowflakeDatabase.getElements(FAKE_TABLE);
    List<String> escapedTestData =
        testDataInStrings.stream()
            .map(e -> e.replace("'", "''"))
            .map(e -> String.format("\"%s\"", e))
            .collect(Collectors.toList());
    assertTrue(TestUtils.areListsEqual(escapedTestData, actualData));
  }

  @Test
  public void writeToExternalWithBlankQuotation() throws SnowflakeSQLException {
    pipeline
        .apply(Create.of(testDataInStrings))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(TestUtils.getStringCsvMapper())
                .to(FAKE_TABLE)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withSnowflakeService(snowflakeService)
                .withQuotationMark(""));

    pipeline.run(options).waitUntilFinish();

    List<String> actualData = FakeSnowflakeDatabase.getElements(FAKE_TABLE);

    List<String> escapedTestData =
        testDataInStrings.stream().map(e -> e.replace("'", "''")).collect(Collectors.toList());
    assertTrue(TestUtils.areListsEqual(escapedTestData, actualData));
  }
}
