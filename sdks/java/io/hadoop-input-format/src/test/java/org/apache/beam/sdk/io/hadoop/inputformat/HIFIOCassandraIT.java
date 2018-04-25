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

import com.datastax.driver.core.Row;
import java.io.Serializable;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO} on an
 * independent Cassandra instance.
 *
 * <p>This test requires a running instance of Cassandra, and the test dataset must exist in
 * the database.
 *
 * <p>You can run this test by doing the following:
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/hadoop-input-format
 *  -Dit.test=org.apache.beam.sdk.io.hadoop.inputformat.HIFIOCassandraIT
 *  -DintegrationTestPipelineOptions='[
 *  "--cassandraServerIp=1.2.3.4",
 *  "--cassandraServerPort=port",
 *  "--cassandraUserName=user",
 *  "--cassandraPassword=mypass" ]'
 *  --tests org.apache.beam.sdk.io.hadoop.inputformat.HIFIOCassandraIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>If you want to run this with a runner besides directrunner, there are profiles for dataflow
 * and spark in the pom. You'll want to activate those in addition to the normal test runner
 * invocation pipeline options.
 */

@RunWith(JUnit4.class)
public class HIFIOCassandraIT implements Serializable {

  private static final String CASSANDRA_KEYSPACE = "ycsb";
  private static final String CASSANDRA_TABLE = "usertable";
  private static final String CASSANDRA_THRIFT_PORT_PROPERTY = "cassandra.input.thrift.port";
  private static final String CASSANDRA_THRIFT_ADDRESS_PROPERTY = "cassandra.input.thrift.address";
  private static final String CASSANDRA_PARTITIONER_CLASS_PROPERTY =
      "cassandra.input.partitioner.class";
  private static final String CASSANDRA_KEYSPACE_PROPERTY = "cassandra.input.keyspace";
  private static final String CASSANDRA_COLUMNFAMILY_PROPERTY = "cassandra.input.columnfamily";
  private static final String CASSANDRA_PARTITIONER_CLASS_VALUE = "Murmur3Partitioner";
  private static final String USERNAME = "cassandra.username";
  private static final String PASSWORD = "cassandra.password";
  private static final String INPUT_KEYSPACE_USERNAME_CONFIG = "cassandra.input.keyspace.username";
  private static final String INPUT_KEYSPACE_PASSWD_CONFIG = "cassandra.input.keyspace.passwd";
  private static HIFITestOptions options;
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFITestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFITestOptions.class);
  }

  /**
   * This test reads data from the Cassandra instance and verifies if data is read successfully.
   */
  @Test
  public void testHIFReadForCassandra() {
    // Expected hashcode is evaluated during insertion time one time and hardcoded here.
    String expectedHashCode = "1a30ad400afe4ebf5fde75f5d2d95408";
    Long expectedRecordsCount = 1000L;
    Configuration conf = getConfiguration(options);
    PCollection<KV<Long, String>> cassandraData = pipeline.apply(HadoopInputFormatIO
        .<Long, String>read().withConfiguration(conf).withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.globally()))
        .isEqualTo(expectedRecordsCount);
    PCollection<String> textValues = cassandraData.apply(Values.create());
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();
  }

  SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
    @Override
    public String apply(Row input) {
      return input.getString("y_id") + "|" + input.getString("field0") + "|"
          + input.getString("field1") + "|" + input.getString("field2") + "|"
          + input.getString("field3") + "|" + input.getString("field4") + "|"
          + input.getString("field5") + "|" + input.getString("field6") + "|"
          + input.getString("field7") + "|" + input.getString("field8") + "|"
          + input.getString("field9");
    }
  };
  /**
   * This test reads data from the Cassandra instance based on query and verifies if data is read
   * successfully.
   */
  @Test
  public void testHIFReadForCassandraQuery() {
    String expectedHashCode = "7bead6d6385c5f4dd0524720cd320b49";
    Long expectedNumRows = 1L;
    Configuration conf = getConfiguration(options);
    conf.set("cassandra.input.cql", "select * from " + CASSANDRA_KEYSPACE + "." + CASSANDRA_TABLE
        + " where token(y_id) > ? and token(y_id) <= ? "
        + "and field0 = 'user48:field0:431531'");
    PCollection<KV<Long, String>> cassandraData =
        pipeline.apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.globally()))
        .isEqualTo(expectedNumRows);
    PCollection<String> textValues = cassandraData.apply(Values.create());
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();
  }

  /**
   * Returns Hadoop configuration for reading data from Cassandra. To read data from Cassandra using
   * HadoopInputFormatIO, following properties must be set: InputFormat class, InputFormat key
   * class, InputFormat value class, Thrift address, Thrift port, partitioner class, keyspace and
   * columnfamily name.
   */
  private static Configuration getConfiguration(HIFITestOptions options) {
    Configuration conf = new Configuration();
    conf.set(CASSANDRA_THRIFT_PORT_PROPERTY, options.getCassandraServerPort().toString());
    conf.set(CASSANDRA_THRIFT_ADDRESS_PROPERTY, options.getCassandraServerIp());
    conf.set(CASSANDRA_PARTITIONER_CLASS_PROPERTY, CASSANDRA_PARTITIONER_CLASS_VALUE);
    conf.set(CASSANDRA_KEYSPACE_PROPERTY, CASSANDRA_KEYSPACE);
    conf.set(CASSANDRA_COLUMNFAMILY_PROPERTY, CASSANDRA_TABLE);
    // Set user name and password if Cassandra instance has security configured.
    conf.set(USERNAME, options.getCassandraUserName());
    conf.set(PASSWORD, options.getCassandraPassword());
    conf.set(INPUT_KEYSPACE_USERNAME_CONFIG, options.getCassandraUserName());
    conf.set(INPUT_KEYSPACE_PASSWD_CONFIG, options.getCassandraPassword());
    conf.setClass("mapreduce.job.inputformat.class",
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
    conf.setClass("key.class", java.lang.Long.class, Object.class);
    conf.setClass("value.class", com.datastax.driver.core.Row.class, Object.class);
    return conf;
  }
}
