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
package org.apache.beam.sdk.io.snowflake.test.unit.read;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBatchServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.TestSnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeIOReadTest implements Serializable {
  public static final String FAKE_TABLE = "FAKE_TABLE";
  public static final String FAKE_QUERY = "SELECT * FROM FAKE_TABLE";
  public static final String BUCKET_NAME = "BUCKET/";

  private static final TestSnowflakePipelineOptions options =
      TestPipeline.testingPipelineOptions().as(TestSnowflakePipelineOptions.class);
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static SnowflakeService snowflakeService;
  private static List<GenericRecord> avroTestData;

  @BeforeClass
  public static void setup() {

    List<String> testData = Arrays.asList("Paul,51,red", "Jackson,41,green");

    avroTestData =
        ImmutableList.of(
            new AvroGeneratedUser("Paul", 51, "red"),
            new AvroGeneratedUser("Jackson", 41, "green"));

    FakeSnowflakeDatabase.createTableWithElements(FAKE_TABLE, testData);

    options.setServerName("NULL.snowflakecomputing.com");
    options.setStorageIntegrationName("STORAGE_INTEGRATION");
    options.setStagingBucketName(BUCKET_NAME);

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());

    snowflakeService = new FakeSnowflakeBatchServiceImpl();
  }

  @AfterClass
  public static void tearDown() {
    TestUtils.removeTempDir(BUCKET_NAME);
  }

  @Test
  public void testConfigIsMissingStagingBucketName() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withStagingBucketName() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingStorageIntegration() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withStorageIntegrationName() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(options.getStagingBucketName())
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingCsvMapper() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withCsvMapper() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(options.getStagingBucketName())
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingCoder() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withCoder() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(options.getStagingBucketName())
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCsvMapper(getCsvMapper()));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingFromTableOrFromQuery() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("fromTable() or fromQuery() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .withStagingBucketName(options.getStagingBucketName())
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingDataSourceConfiguration() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withDataSourceConfiguration() or withDataSourceProviderFn() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(options.getStagingBucketName())
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigContainsFromQueryAndFromTable() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("fromTable() and fromQuery() are not allowed together");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromQuery("")
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(options.getStagingBucketName())
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testTableDoesntExist() {
    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage("SQL compilation error: Table does not exist");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable("NON_EXIST")
            .withStagingBucketName(options.getStagingBucketName())
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testInvalidQuery() {
    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage("SQL compilation error: Invalid query");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromQuery("BAD_QUERY")
            .withStagingBucketName(options.getStagingBucketName())
            .withStorageIntegrationName(options.getStorageIntegrationName())
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testReadFromTable() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read(snowflakeService)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(FAKE_TABLE)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withCsvMapper(getCsvMapper())
                .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    PAssert.that(items).containsInAnyOrder(avroTestData);
    pipeline.run();
  }

  @Test
  public void testReadFromQuery() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read(snowflakeService)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromQuery(FAKE_QUERY)
                .withStagingBucketName(options.getStagingBucketName())
                .withStorageIntegrationName(options.getStorageIntegrationName())
                .withCsvMapper(getCsvMapper())
                .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    PAssert.that(items).containsInAnyOrder(avroTestData);
    pipeline.run();
  }

  static SnowflakeIO.CsvMapper<GenericRecord> getCsvMapper() {
    return (SnowflakeIO.CsvMapper<GenericRecord>)
        parts ->
            new GenericRecordBuilder(AvroGeneratedUser.getClassSchema())
                .set("name", String.valueOf(parts[0]))
                .set("favorite_number", Integer.valueOf(parts[1]))
                .set("favorite_color", String.valueOf(parts[2]))
                .build();
  }
}
