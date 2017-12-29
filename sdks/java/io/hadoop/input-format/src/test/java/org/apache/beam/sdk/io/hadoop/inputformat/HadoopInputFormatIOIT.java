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
package org.apache.beam.sdk.io.hadoop.inputformat;

import static org.apache.beam.sdk.io.common.TestRow.DeterministicallyConstructTestRowFn;
import static org.apache.beam.sdk.io.common.TestRow.SelectNameFn;
import static org.apache.beam.sdk.io.hadoop.inputformat.TestRowDBWritable.PrepareStatementFromTestRow;

import java.sql.SQLException;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * IOIT for {@link org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO}.
 */
public class HadoopInputFormatIOIT {

  private static PGSimpleDataSource dataSource;
  private static Long numberOfRows;
  private static String tableName;
  private static SerializableConfiguration hadoopConfiguration;

  @Rule
  public TestPipeline writePipeline = TestPipeline.create();

  @Rule
  public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() throws SQLException {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    numberOfRows = options.getNumberOfRecords();
    tableName = DatabaseTestHelper.getTestTableName("HadoopInputFormatIOIT");

    DatabaseTestHelper.createDataTable(dataSource, tableName);
    setupHadoopConfiguration(options);
  }

  private static void setupHadoopConfiguration(IOTestPipelineOptions options) {
    Configuration conf = new Configuration();
    DBConfiguration.configureDB(
        conf,
        "org.postgresql.Driver",
        DatabaseTestHelper.getPostgresDBUrl(options),
        options.getPostgresUsername(),
        options.getPostgresPassword()
    );
    conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
    conf.setStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, "id", "name");
    conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, TestRowDBWritable.class, DBWritable.class);

    conf.setClass("key.class", LongWritable.class, Object.class);
    conf.setClass("value.class", TestRowDBWritable.class, Object.class);
    conf.setClass("mapreduce.job.inputformat.class", DBInputFormat.class, InputFormat.class);

    hadoopConfiguration = new SerializableConfiguration(conf);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    DatabaseTestHelper.cleanUpDataTable(dataSource, tableName);
  }

  @Test
  public void writeThenReadUsingDBInputFormat() {
    writePipeline.apply("Generate sequence", GenerateSequence.from(0).to(numberOfRows))
        .apply("Produce db rows", ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply(JdbcIO.<TestRow>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withStatement(String.format("insert into %s values(?, ?)", tableName))
            .withPreparedStatementSetter(new PrepareStatementFromTestRow()));


    writePipeline.run().waitUntilFinish();

    PCollection<String> consolidatedHashcode = readPipeline
        .apply("Read using DBInputFormat", HadoopInputFormatIO
            .<LongWritable, TestRowDBWritable>read()
            .withConfiguration(hadoopConfiguration.get()))
        .apply("Get values only", Values.<TestRowDBWritable>create())
        .apply("Values as string", ParDo.of(new SelectNameFn()))
        .apply("Calculate hashcode", Combine.globally(new HashingFn()))
        .apply(Reshuffle.<String>viaRandomKey());

    PAssert.thatSingleton(consolidatedHashcode)
        .isEqualTo(TestRow.getExpectedHashForRowCount(numberOfRows.intValue()));

    readPipeline.run().waitUntilFinish();
  }
}
