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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOContants;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs integration test to validate HadoopInputFromatIO for an Elasticsearch instance on GCP.
 *
 * You need to pass Elasticsearch server IP and port in beamTestPipelineOptions
 *
 * <p>
 * You can run just this test by doing the following:
 * mvn test-compile compile failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]'
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOElasticIT implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOElasticIT.class);
  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "test_data";
  private static final String ELASTIC_TYPE_NAME = "test_type";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;
  private static HIFTestOptions options;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
    LOGGER.info("Pipeline created successfully with the options");
  }

  /**
   * This test reads data from the Elasticsearch instance and verifies whether data is read successfully.
   */
  @Test
  public void testHifIOWithElastic() {
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    PCollection<KV<Text, MapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, MapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, MapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 1000);
    pipeline.run().waitUntilFinish();
  }

  /**
   * This test reads data from the Elasticsearch instance based on a query and verifies if data is read
   * successfully.
   */
  @Test
  public void testHifIOWithElasticQuery() {
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    String query =
          "{" + "  \"query\": {"
              + "  \"match\" : {"
              + "    \"Item_Code\" : {"
              + "      \"query\" : \"86345\","
              + "      \"type\" : \"boolean\""
              + "    }"
              + "  }"
              + "  }"
              + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, MapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, MapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, MapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 1);
    pipeline.run().waitUntilFinish();
  }

 /**
	 * Set the Elasticsearch configuration parameters in the Hadoop
	 * configuration object. Configuration object should have InputFormat class,
	 * key class and value class to be set Mandatory fields for ESInputFormat to
	 * be set are es.resource, es.nodes, es.port, es.internal.es.version
	 */
  public static Configuration getConfiguration(HIFTestOptions options) {
    Configuration conf = new Configuration();
    conf.set(ConfigurationOptions.ES_NODES, options.getServerIp());
    conf.set(ConfigurationOptions.ES_PORT, String.format("%d", options.getServerPort()));
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, TRUE);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOContants.KEY_CLASS, Text.class, Object.class);
    conf.setClass(HadoopInputFormatIOContants.VALUE_CLASS, MapWritable.class, Object.class);
    conf.setClass("mapred.mapoutput.value.class", MapWritable.class, Object.class);
    return conf;
  }
}
