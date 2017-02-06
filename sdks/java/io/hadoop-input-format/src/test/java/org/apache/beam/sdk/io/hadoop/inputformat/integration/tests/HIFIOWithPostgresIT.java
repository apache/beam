/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOConstants;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.DBInputWritable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Runs integration test to validate HadoopInputFromatIO for Postgres instance on GCP.
 * <P>
 * In {@link DBInputFormat}, key class is LongWritable, and value class can be anything which
 * extends DBWritable class. You have to provide your own custom value class which extends
 * {@link org.apache.hadoop.mapreduce.lib.db.DBWritable DBWritable} using property
 * "mapreduce.jdbc.input.class". For serialization and deserialization in Beam, custom value class
 * must have an empty public constructor and must implement methods readFields(DataInput in) and
 * write(DataOutput out).
 * <P>
 * Please refer sample custom value class example {@link DBInputWritable} which is input to this
 * test.
 * 
 * <h3>Hadoop configuration for DBInputFormat</h3 You can set value class directly by setting
 * property "mapreduce.jdbc.input.class" as set in this test {@link #getPostgresConfiguration()}.
 * Another common way to set output class is as follows:
 * 
 * <pre>
 * {@code
 * DBInputFormat.setInput(job,
 *    DBInputWritable.class, // Custom value class.
 *    "tableName", // Input table name.
 *    null, 
 *    null, 
 *    new String[] {"column1", "column2"}); // Table columns
 * }
 * </pre>
 * <p>
 * You can run this test by using the following maven command: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>", "--userName=xyz", "--password=root" ]' -Dit.test=HIFIOWithPostgresIT
 * -DskipITs=false
 *
 */
@RunWith(JUnit4.class)
public class HIFIOWithPostgresIT implements Serializable {
  private static final String DRIVER_CLASS_PROPERTY = "org.postgresql.Driver";
  private static String URL_PROPERTY = "jdbc:postgresql://";
  private static final String INPUT_TABLE_NAME_PROPERTY = "scientists";
  private static final String DATABASE_NAME = "beamdb";
  private static final long COUNT_RECORDS = 10L;

  @Rule
  public final TestPipeline p = TestPipeline.create();
  private static HIFTestOptions options;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
    URL_PROPERTY +=
        options.getServerIp() + ":" + String.format("%d", options.getServerPort()) + "/"
            + DATABASE_NAME;
  }

  @Test
  public void testReadData() throws IOException, InstantiationException, IllegalAccessException,
      ClassNotFoundException, InterruptedException {
    Configuration conf = getPostgresConfiguration();
    PCollection<KV<LongWritable, DBInputWritable>> postgresData =
        p.apply(HadoopInputFormatIO.<LongWritable, DBInputWritable>read().withConfiguration(conf));
    PAssert.thatSingleton(
        postgresData.apply("Count", Count.<KV<LongWritable, DBInputWritable>>globally()))
        .isEqualTo(COUNT_RECORDS);
    List<KV<LongWritable, DBInputWritable>> expectedResults =
        Arrays.asList(KV.of(new LongWritable(0L), new DBInputWritable("Faraday", "1")),
            KV.of(new LongWritable(1L), new DBInputWritable("Newton", "2")),
            KV.of(new LongWritable(2L), new DBInputWritable("Galilei", "3")),
            KV.of(new LongWritable(3L), new DBInputWritable("Maxwell", "4")),
            KV.of(new LongWritable(4L), new DBInputWritable("Pasteur", "5")),
            KV.of(new LongWritable(5L), new DBInputWritable("Copernicus", "6")),
            KV.of(new LongWritable(6L), new DBInputWritable("Curie", "7")),
            KV.of(new LongWritable(7L), new DBInputWritable("Bohr", "8")),
            KV.of(new LongWritable(8L), new DBInputWritable("Darwin", "9")),
            KV.of(new LongWritable(9L), new DBInputWritable("Einstein", "10")));
    PAssert.that(postgresData).containsInAnyOrder(expectedResults);
    p.run();
  }

  /**
   * Returns Hadoop configuration for reading data from Postgres. To read data from Postgres using
   * HadoopInputFormatIO, following properties must be set- driver class, jdbc url, username,
   * password, table name, query and value type.
   */
  private static Configuration getPostgresConfiguration() throws IOException {
    Configuration conf = new Configuration();
    conf.set("mapreduce.jdbc.driver.class", DRIVER_CLASS_PROPERTY);
    conf.set("mapreduce.jdbc.url", URL_PROPERTY);
    conf.set("mapreduce.jdbc.username", options.getUserName());
    conf.set("mapreduce.jdbc.password", options.getPassword());
    conf.set("mapreduce.jdbc.input.table.name", INPUT_TABLE_NAME_PROPERTY);
    conf.set("mapreduce.jdbc.input.query", "SELECT * FROM " + INPUT_TABLE_NAME_PROPERTY);
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME, DBInputFormat.class,
        InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, LongWritable.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, DBInputWritable.class, Object.class);
    conf.setClass("mapreduce.jdbc.input.class", DBInputWritable.class, Object.class);
    return conf;
  }
}
