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

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOConstants;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Runs test to validate HadoopInputFromatIO for a HBase instance on GCP.
 *
 * To read data from HBase {@link org.apache.hadoop.hbase.mapreduce.TableInputFormat.class
 * TableInputFormat} can be used. You need to pass HBase server IP and port in
 * beamTestPipelineOptions.
 *
 * <p>
 * You can run just this test by using the following maven command: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]' -Dit.test=HIFIOHBaseIT -DskipITs=false
 *
 */
@RunWith(JUnit4.class)
public class HIFIOHBaseIT implements Serializable {
  private static HIFTestOptions options;
  private static final String TABLE_NAME = "scientists";
  private static final long COUNT_RECORDS = 50L;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
  }
  
  /*
   * This test reads data from the HBase and verifies if data is read successfully.
   */
  @Test
  public void testHifReadWithHBase() throws Throwable {
    TestPipeline p = TestPipeline.create();
    Configuration conf = getHBaseConfiguration();
    SimpleFunction<Result, String> myValueTranslate = new SimpleFunction<Result, String>() {
      @Override
      public String apply(Result input) {
        return Bytes.toString(input.getValue(Bytes.toBytes("info"), Bytes.toBytes("scientist")));
      }
    };
    PCollection<KV<ImmutableBytesWritable, String>> hbaseData =
        p.apply(HadoopInputFormatIO.<ImmutableBytesWritable, String>read()
            .withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert
        .thatSingleton(
            hbaseData.apply("Count", Count.<KV<ImmutableBytesWritable, String>>globally()))
        .isEqualTo(COUNT_RECORDS);
    PCollection<String> values = hbaseData.apply(Values.<String>create());
    List<String> expectedValues = Arrays.asList("Einstein", "Darwin", "Copernicus", "Pasteur",
        "Curie", "Faraday", "Newton", "Bohr", "Galilei", "Maxwell");
    PAssert.that(values).containsInAnyOrder(expectedValues);
    p.run().waitUntilFinish();
	}

  /*
   * Returns Hadoop configuration for reading data from HBase. To read data from HBase using
   * HadoopInputFormatIO, following properties must be set: InputFormat class, InputFormat key
   * class, InputFormat value class, ZooKeeper address, ZooKeeper client port and table name.
   */
  private Configuration getHBaseConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", options.getServerIp());
    conf.set("hbase.zookeeper.property.clientPort", String.format("%d", options.getServerPort()));
    conf.set("hbase.mapreduce.inputtable", TABLE_NAME);
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        org.apache.hadoop.hbase.mapreduce.TableInputFormat.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, ImmutableBytesWritable.class,
        Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS,
        org.apache.hadoop.hbase.client.Result.class, Object.class);
    return conf;
	}
}
