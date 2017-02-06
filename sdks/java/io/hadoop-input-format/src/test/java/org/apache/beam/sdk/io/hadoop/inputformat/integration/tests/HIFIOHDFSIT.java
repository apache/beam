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
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Runs test to validate HadoopInputFromatIO for reading data from HDFS on GCP.
 *
 * This test reads data from HDFS using {@link TextInputFormat.class TextInputFormat}. You need to
 * pass HDFS NameNode IP and NameNode metadata service port in beamTestPipelineOptions.
 *
 * <p>
 * You can run just this test by using the following maven command: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]' -Dit.test=HIFIOHDFSIT -DskipITs=false
 *
 */
@RunWith(JUnit4.class)
public class HIFIOHDFSIT implements Serializable {
  private static HIFTestOptions options;
  private static final String FILE_NAME = "scintistData.txt";
  private static String filePath = "hdfs://";
  private static final long COUNT_RECORDS = 10L;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
    filePath +=
        options.getServerIp() + ":" + String.format("%d", options.getServerPort()) + "/"
            + FILE_NAME;
  }

  /**
   * This test reads data from HDFS and verifies if data is read successfully.
   */
  @Test
  public void testHifReadWithHDFS() throws Throwable {
    TestPipeline p = TestPipeline.create();
    Configuration conf = getHDFSConfiguration();
    PCollection<KV<LongWritable, Text>> hdfsData =
        p.apply(HadoopInputFormatIO.<LongWritable, Text>read().withConfiguration(conf));
    List<Text> expectedValues = Arrays.asList(
        new Text("Einstein"), 
        new Text("Darwin"), 
        new Text("Copernicus"), 
        new Text("Pasteur"),
        new Text("Curie"), 
        new Text("Faraday"), 
        new Text("Newton"), 
        new Text("Bohr"), 
        new Text("Galilei"), 
        new Text("Maxwell"));
    PCollection<Text> values = hdfsData.apply(Values.<Text>create());
    PAssert.thatSingleton(hdfsData.apply("Count", Count.<KV<LongWritable, Text>>globally()))
        .isEqualTo(COUNT_RECORDS);
    PAssert.that(values).containsInAnyOrder(expectedValues);
    p.run().waitUntilFinish();
  }

  /**
   * Returns Hadoop configuration of reading data from HDFS. To read data from HDFS, following
   * properties must be set: NameNode IP address, NameNode metadata service port and file name.
   * You can control size of split using property "mapred.max.split.size".
   */
  private Configuration getHDFSConfiguration() {
    Configuration conf = new Configuration();
    conf.set("mapred.input.dir", StringUtils.escapeString(filePath));
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME, TextInputFormat.class,
        Object.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, LongWritable.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, Text.class, Object.class);
    return conf;
  }
}
