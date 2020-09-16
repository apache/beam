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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTime;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeVariant;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeText;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBatchServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.TestSnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaDispositionTest {
  private static final String FAKE_TABLE = "FAKE_TABLE";
  private static final String BUCKET_NAME = "BUCKET/";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private static TestSnowflakePipelineOptions options;
  private static SnowflakeIO.DataSourceConfiguration dc;
  private static String stagingBucketName;
  private static String storageIntegrationName;

  private static SnowflakeService snowflakeService;

  @BeforeClass
  public static void setupAll() {
    PipelineOptionsFactory.register(TestSnowflakePipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TestSnowflakePipelineOptions.class);
    options.setStagingBucketName(BUCKET_NAME);
    options.setServerName("NULL.snowflakecomputing.com");

    stagingBucketName = options.getStagingBucketName();
    storageIntegrationName = options.getStorageIntegrationName();

    snowflakeService = new FakeSnowflakeBatchServiceImpl();

    dc =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());
  }

  @Before
  public void setup() {}

  @After
  public void tearDown() {
    TestUtils.removeTempDir(BUCKET_NAME);
    FakeSnowflakeDatabase.clean();
  }

  public static SnowflakeIO.UserDataMapper<String[]> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<String[]>) recordLine -> recordLine;
  }

  @Test
  public void writeWithCreatedTableWithDatetimeSchemaSuccess() throws SQLException {

    List<String[]> testDates =
        LongStream.range(0, 100)
            .boxed()
            .map(num -> new String[] {"2020-08-25", "2014-01-01 16:00:00", "00:02:03"})
            .collect(Collectors.toList());
    List<String> testDatesSnowflakeFormat =
        testDates.stream().map(TestUtils::toSnowflakeRow).collect(Collectors.toList());

    SnowflakeTableSchema tableSchema =
        new SnowflakeTableSchema(
            SnowflakeColumn.of("date", SnowflakeDate.of()),
            SnowflakeColumn.of("datetime", SnowflakeDateTime.of()),
            SnowflakeColumn.of("time", SnowflakeTime.of()));

    pipeline
        .apply(Create.of(testDates))
        .apply(
            "Copy IO",
            SnowflakeIO.<String[]>write()
                .withDataSourceConfiguration(dc)
                .to("NO_EXIST_TABLE")
                .withTableSchema(tableSchema)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withFileNameTemplate("output")
                .withUserDataMapper(TestUtils.getLStringCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<String> actualData = FakeSnowflakeDatabase.getElements("NO_EXIST_TABLE");
    assertTrue(TestUtils.areListsEqual(testDatesSnowflakeFormat, actualData));
  }

  @Test
  public void writeWithCreatedTableWithNullValuesInSchemaSuccess() throws SnowflakeSQLException {

    List<String[]> testNulls =
        LongStream.range(0, 100)
            .boxed()
            .map(num -> new String[] {null, null, null})
            .collect(Collectors.toList());
    List<String> testNullsSnowflakeFormat =
        testNulls.stream().map(TestUtils::toSnowflakeRow).collect(Collectors.toList());

    SnowflakeTableSchema tableSchema =
        new SnowflakeTableSchema(
            SnowflakeColumn.of("date", SnowflakeDate.of(), true),
            new SnowflakeColumn("datetime", SnowflakeDateTime.of(), true),
            SnowflakeColumn.of("text", SnowflakeText.of(), true));

    pipeline
        .apply(Create.of(testNulls))
        .apply(
            "Copy IO",
            SnowflakeIO.<String[]>write()
                .withDataSourceConfiguration(dc)
                .to("NO_EXIST_TABLE")
                .withTableSchema(tableSchema)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withFileNameTemplate("output")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<String> actualData = FakeSnowflakeDatabase.getElements("NO_EXIST_TABLE");
    assertTrue(TestUtils.areListsEqual(testNullsSnowflakeFormat, actualData));
  }

  @Test
  public void writeWithCreatedTableWithStructuredDataSchemaSuccess() throws SQLException {
    String json = "{ \"key1\": 1, \"key2\": {\"inner_key\": \"value2\", \"inner_key2\":18} }";
    String array = "[1,2,3]";

    List<String[]> testStructuredData =
        LongStream.range(0, 100)
            .boxed()
            .map(num -> new String[] {json, array, json})
            .collect(Collectors.toList());
    List<String> testStructuredDataSnowflakeFormat =
        testStructuredData.stream().map(TestUtils::toSnowflakeRow).collect(Collectors.toList());

    SnowflakeTableSchema tableSchema =
        new SnowflakeTableSchema(
            SnowflakeColumn.of("variant", SnowflakeArray.of()),
            SnowflakeColumn.of("object", SnowflakeObject.of()),
            SnowflakeColumn.of("array", SnowflakeVariant.of()));

    pipeline
        .apply(Create.of(testStructuredData))
        .apply(
            "Copy IO",
            SnowflakeIO.<String[]>write()
                .withDataSourceConfiguration(dc)
                .to("NO_EXIST_TABLE")
                .withTableSchema(tableSchema)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withFileNameTemplate("output")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<String> actualData = FakeSnowflakeDatabase.getElements("NO_EXIST_TABLE");
    assertTrue(TestUtils.areListsEqual(testStructuredDataSnowflakeFormat, actualData));
  }
}
