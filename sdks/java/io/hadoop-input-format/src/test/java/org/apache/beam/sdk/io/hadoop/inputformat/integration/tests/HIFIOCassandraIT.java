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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOConstants;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

/**
 * Runs integration test to validate HadoopInputFromatIO for a Cassandra instance.
 *
 * You need to pass Cassandra server IP and port in beamTestPipelineOptions.
 *
 * <p>
 * You can run just this test by doing the following: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]' -Dit.test=HIFIOCassandraIT -DskipITs=false
 */
@RunWith(JUnit4.class)
public class HIFIOCassandraIT implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOCassandraIT.class);
  private static final String CASSANDRA_KEYSPACE = "beamdb";
  private static final String CASSANDRA_TABLE = "scientists";
  private static HIFTestOptions options;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
  }

  /**
   * This test reads data from the Cassandra instance and verifies if data is read successfully.
   */
  @Test
  public void testHIFReadForCassandra() {
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      @Override
      public String apply(Row input) {
        return input.getString("scientist");
      }
    };
    PCollection<KV<Long, String>> cassandraData = pipeline
                    .apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
                        .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(10L);

    List<KV<Long, String>> expectedResults =
        Arrays.asList(KV.of(1L, "Faraday"), KV.of(2L, "Newton"), KV.of(3L, "Galilei"),
            KV.of(4L, "Maxwell"), KV.of(5L, "Pasteur"), KV.of(6L, "Copernicus"),
            KV.of(7L, "Curie"), KV.of(8L, "Bohr"), KV.of(9L, "Darwin"), KV.of(10L, "Einstein"));
    PAssert.that(cassandraData).containsInAnyOrder(expectedResults);
    pipeline.run().waitUntilFinish();
  }

  /**
   * This test reads data from the Cassandra instance based on query and verifies if data is read
   * successfully.
   */
  @Test
  public void testHIFReadForCassandraQuery() {
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    conf.set("cassandra.input.cql", "select * from " + CASSANDRA_KEYSPACE + "." + CASSANDRA_TABLE
        + " where token(id) > ? and token(id) <= ? and scientist='Einstein' allow filtering");
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      @Override
      public String apply(Row input) {
        return input.getString("id");
      }
    };
    PCollection<KV<Long, String>> cassandraData = pipeline
                    .apply(HadoopInputFormatIO.<Long, String>read()
                        .withConfiguration(conf)
                        .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(1L);

    pipeline.run().waitUntilFinish();
  }

  /**
   * Returns configuration of CqlInutFormat. Mandatory parameters required apart from inputformat
   * class name, key class, value class are thrift port, thrift address, partitioner class, keyspace
   * and columnfamily name.
   */
  public static Configuration getConfiguration(HIFTestOptions options) {
    Configuration conf = new Configuration();
    conf.set("cassandra.input.thrift.port", options.getServerPort().toString());
    conf.set("cassandra.input.thrift.address", options.getServerIp());
    conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
    conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
    conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, java.lang.Long.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, com.datastax.driver.core.Row.class,
        Object.class);
    return conf;
  }
}
