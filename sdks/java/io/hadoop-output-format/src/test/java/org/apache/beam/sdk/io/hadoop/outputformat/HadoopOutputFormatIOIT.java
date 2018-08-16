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
package org.apache.beam.sdk.io.hadoop.outputformat;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;

import java.sql.SQLException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.inputformat.TestRowDBWritable;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * A test of {@link org.apache.beam.sdk.io.hadoop.outputformat.HadoopOutputFormatIO} on an
 * independent postgres instance.
 *
 * <p>This test requires a running instance of Postgres. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/hadoop-output-format/
 *   -DintegrationTestPipelineOptions='[
 *     "--postgresServerName=1.2.3.4",
 *     "--postgresUsername=postgres",
 *     "--postgresDatabaseName=myfancydb",
 *     "--postgresPassword=mypass",
 *     "--postgresSsl=false",
 *     "--numberOfRecords=1000" ]'
 *  --tests org.apache.beam.sdk.io.hadoop.outputformat.HadoopOutputFormatIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
public class HadoopOutputFormatIOIT {

  private static PGSimpleDataSource dataSource;
  private static Integer numberOfRows;
  private static String tableName;
  private static SerializableConfiguration hadoopConfiguration;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() throws Exception {
    PostgresIOTestPipelineOptions options =
        readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);

    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    numberOfRows = options.getNumberOfRecords();
    tableName = DatabaseTestHelper.getTestTableName("HadoopOutputFormatIOIT");

    executeWithRetry(HadoopOutputFormatIOIT::createTable);
    setupHadoopConfiguration(options);
  }

  private static void createTable() throws SQLException {
    DatabaseTestHelper.createTable(dataSource, tableName);
  }

  private static void setupHadoopConfiguration(PostgresIOTestPipelineOptions options) {
    Configuration conf = new Configuration();
    DBConfiguration.configureDB(
        conf,
        "org.postgresql.Driver",
        DatabaseTestHelper.getPostgresDBUrl(options),
        options.getPostgresUsername(),
        options.getPostgresPassword());
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
    conf.set(DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY, "2");
    conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, "id", "name");

    conf.setClass(
        HadoopOutputFormatIO.OUTPUTFORMAT_KEY_CLASS, TestRowDBWritable.class, Object.class);
    conf.setClass(HadoopOutputFormatIO.OUTPUTFORMAT_VALUE_CLASS, NullWritable.class, Object.class);
    conf.setClass(
        HadoopOutputFormatIO.OUTPUTFORMAT_CLASS, DBOutputFormat.class, OutputFormat.class);

    hadoopConfiguration = new SerializableConfiguration(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    executeWithRetry(HadoopOutputFormatIOIT::deleteTable);
  }

  private static void deleteTable() throws SQLException {
    DatabaseTestHelper.deleteTable(dataSource, tableName);
  }

  @Test
  public void writeUsingHadoopOutputFormat() {
    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRows))
        .apply("Produce db rows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply("Construct rows for DBOutputFormat", ParDo.of(new ConstructDBOutputFormatRowFn()))
        .apply(
            "Write using HadoopOutputFormat",
            HadoopOutputFormatIO.<TestRowDBWritable, NullWritable>write()
                .withConfiguration(hadoopConfiguration.get()));

    writePipeline.run().waitUntilFinish();

    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply(
                "Read using JDBCIO",
                JdbcIO.<String>read()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                    .withQuery(String.format("select name,id from %s;", tableName))
                    .withRowMapper(
                        (JdbcIO.RowMapper<String>) resultSet -> resultSet.getString("name"))
                    .withCoder(StringUtf8Coder.of()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(getExpectedHashForRowCount(numberOfRows));

    readPipeline.run().waitUntilFinish();
  }

  /**
   * Uses the input {@link TestRow} values as seeds to produce new {@link KV}s for {@link
   * HadoopOutputFormatIO}.
   */
  public static class ConstructDBOutputFormatRowFn
      extends DoFn<TestRow, KV<TestRowDBWritable, NullWritable>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(
          KV.of(new TestRowDBWritable(c.element().id(), c.element().name()), NullWritable.get()));
    }
  }
}
