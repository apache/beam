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

import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.snowflake.SnowflakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.unit.BatchTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

@RunWith(JUnit4.class)
public class SnowflakeIOReadTest {
  public static final String FAKE_TABLE = "FAKE_TABLE";

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static BatchTestPipelineOptions options;

  private static SnowflakeService snowflakeService;
  private static SnowflakeCloudProvider snowflakeCloudProvider;

  private static String stagingBucketName;
  private static String integrationName;

  private static List<GenericRecord> avroTestData;

  private transient TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() {

    List<String> testData = Arrays.asList("Paul,51,red", "Jackson,41,green");

    avroTestData =
        ImmutableList.of(
            new AvroGeneratedUser("Paul", 51, "red"),
            new AvroGeneratedUser("Jackson", 41, "green"));

    FakeSnowflakeDatabase.createTableWithElements(FAKE_TABLE, testData);
    PipelineOptionsFactory.register(BatchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BatchTestPipelineOptions.class);
    options.setServerName("NULL.snowflakecomputing.com");
    options.setStorageIntegration("STORAGE_INTEGRATION");
    options.setStagingBucketName("BUCKET");

    stagingBucketName = options.getStagingBucketName();
    integrationName = options.getStorageIntegration();

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());

    snowflakeService = new FakeSnowflakeServiceImpl();
    snowflakeCloudProvider = new FakeSnowflakeCloudProvider();
  }

  @Rule
  public final transient TestRule folderThenPipeline =
      new TestRule() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() {
                  pipeline = TestPipeline.fromOptions(options);
                }
              };
          return testFolder.apply(withPipeline, description);
        }
      };

  @Test
  public void testConfigIsMissingStagingBucketName() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("withStagingBucketName() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingIntegrationName() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("withIntegrationName() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingCsvMapper() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("withCsvMapper() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingCoder() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("withCoder() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper()));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingFromTableOrFromQuery() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("fromTable() or fromQuery() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigIsMissingDataSourceConfiguration() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage(
        "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testConfigContainsFromQueryAndFromTable() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("fromTable() and fromQuery() are not allowed together");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromQuery("")
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testTableDoesntExist() {
    exceptionRule.expect(PipelineExecutionException.class);
    exceptionRule.expectMessage("SQL compilation error: Table does not exist");

    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable("NON_EXIST")
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run();
  }

  @Test
  public void testReadWithConfigIsProper() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read(snowflakeService, snowflakeCloudProvider)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(FAKE_TABLE)
                .withStagingBucketName(stagingBucketName)
                .withIntegrationName(integrationName)
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
