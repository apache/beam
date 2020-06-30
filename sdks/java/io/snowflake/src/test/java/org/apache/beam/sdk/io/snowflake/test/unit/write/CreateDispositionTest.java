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

import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBatchServiceImpl;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getCsvMapper;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
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
public class CreateDispositionTest {
  private static final String FAKE_TABLE = "FAKE_TABLE";
  private static final String BUCKET_NAME = "bucket/";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private static SnowflakePipelineOptions options;
  private static SnowflakeIO.DataSourceConfiguration dc;
  private static String stagingBucketName;
  private static String storageIntegrationName;

  private static SnowflakeService snowflakeService;
  private static List<Long> testData;

  @BeforeClass
  public static void setupAll() {
    PipelineOptionsFactory.register(SnowflakePipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SnowflakePipelineOptions.class);
    options.setStagingBucketName(BUCKET_NAME);
    options.setServerName("NULL.snowflakecomputing.com");

    stagingBucketName = options.getStagingBucketName();
    storageIntegrationName = options.getStorageIntegrationName();

    snowflakeService = new FakeSnowflakeBatchServiceImpl();
    testData = LongStream.range(0, 100).boxed().collect(Collectors.toList());

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

  @Test
  public void writeWithWriteCreateDispositionWithAlreadyCreatedTableSuccess() throws SQLException {
    FakeSnowflakeDatabase.createTable(FAKE_TABLE);

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(FAKE_TABLE)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withFileNameTemplate("output")
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.areListsEqual(testData, actualData));
  }

  @Test
  public void writeWithWriteCreateDispositionWithCreatedTableWithoutSchemaFails() {

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("NO_EXIST_TABLE")
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withFileNameTemplate("output")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();
  }

  @Test
  public void writeWithWriteCreateDispositionWithCreatedTableWithSchemaSuccess()
      throws SQLException {
    SnowflakeTableSchema tableSchema =
        new SnowflakeTableSchema(SnowflakeColumn.of("id", new SnowflakeVarchar()));

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("NO_EXIST_TABLE")
                .withTableSchema(tableSchema)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withFileNameTemplate("output")
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();
    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong("NO_EXIST_TABLE");

    assertTrue(TestUtils.areListsEqual(testData, actualData));
  }

  @Test
  public void writeWithWriteCreateDispositionWithCreateNeverSuccess() throws SQLException {
    FakeSnowflakeDatabase.createTable(FAKE_TABLE);

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(FAKE_TABLE)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withFileNameTemplate("output")
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();
    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.areListsEqual(testData, actualData));
  }

  @Test
  public void writeWithWriteCreateDispositionWithCreateNeededFails() {

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("SQL compilation error: Table does not exist");

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("NO_EXIST_TABLE")
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withFileNameTemplate("output")
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withSnowflakeService(snowflakeService));

    pipeline.run(options).waitUntilFinish();
  }
}
