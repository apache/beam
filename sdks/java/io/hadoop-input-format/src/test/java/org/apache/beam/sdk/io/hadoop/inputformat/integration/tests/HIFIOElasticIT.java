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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOConstants;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.io.hadoop.inputformat.hashing.HashingFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Runs integration test to validate HadoopInputFromatIO for an Elasticsearch instance.
 * You need to pass Elasticsearch server IP and port in beamTestPipelineOptions.
 * <p>You can run just this test by doing the following: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4", 
 * "--serverPort=<port>", "--userName=<user_name>", "--password=<password>"]' 
 * -Dit.test=HIFIOElasticIT -DskipITs=false
 * Setting username and password is optional, set these only if security is 
 * configured on Elasticsearch server.</p>
 */
@RunWith(JUnit4.class)
public class HIFIOElasticIT implements Serializable {

  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "test_data";
  private static final String ELASTIC_TYPE_NAME = "test_type";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;
  private static HIFTestOptions options;
  private static final long TEST_DATA_ROW_COUNT = 1000L;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
  }

  /**
   * This test reads data from the Elasticsearch instance and verifies whether data is read
   * successfully.
   * @throws IOException 
   * @throws SecurityException 
   */
  @Test
  public void testHifIOWithElastic() throws SecurityException, IOException {
    // Expected hashcode is evaluated during insertion time one time and hardcoded here.
    String expectedHashCode = "7373697a12faa08be32104f67cf7ec2be2e20a1f";
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo(TEST_DATA_ROW_COUNT);
    PCollection<LinkedMapWritable> values = esData.apply(Values.<LinkedMapWritable>create());
    MapElements<LinkedMapWritable, String> transformFunc =
        MapElements.<LinkedMapWritable, String>via(new SimpleFunction<LinkedMapWritable, String>() {
          @Override
          public String apply(LinkedMapWritable mapw) {
            String rowValue = "";
            rowValue = convertMapWRowToString(mapw, rowValue);
            return rowValue;
          }
          
        });

    PCollection<String> textValues = values.apply(transformFunc);
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();
  }
  
  /*
   * Function to create a toString implementation of a MapWritable row by writing all field values
   * in a string row.
   */
  private String convertMapWRowToString(LinkedMapWritable mapw, String rowValue) {
    rowValue = addFieldValuesToRow(rowValue, mapw, "User_Name");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Code");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Txn_ID");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Item_ID");
    rowValue = addFieldValuesToRow(rowValue, mapw, "last_updated");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Price");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Title");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Description");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Age");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Name");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Price");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Availability");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Batch_Num");
    rowValue = addFieldValuesToRow(rowValue, mapw, "Last_Ordered");
    rowValue = addFieldValuesToRow(rowValue, mapw, "City");
    return rowValue;
  }
  
  /*
   * Convert a MapWritable row field into a string, and append it to the row string with a
   * separator.
   */
  private String addFieldValuesToRow(String row, MapWritable mapw, String columnName) {
    Object valueObj = (Object) mapw.get(new Text(columnName));
    row += valueObj.toString() + "|";
    return row;
  }

  /**
   * This test reads data from the Elasticsearch instance based on a query and verifies if data is
   * read successfully.
   */
  @Test
  public void testHifIOWithElasticQuery() {
    String expectedHashCode = "abfc29069634f6e9d02a10129ae476e114f15448";
    Long expectedRecords=1L;
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    String query =
        "{" + "  \"query\": {" 
            + "  \"match\" : {" 
            + "    \"Title\" : {"
            + "      \"query\" : \"M9u5xcAR\"," 
            + "      \"type\" : \"boolean\"" 
            + "    }" 
            + "  }"
            + "  }" 
            + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PAssert.thatSingleton(count).isEqualTo(expectedRecords);
    PCollection<LinkedMapWritable> values = esData.apply(Values.<LinkedMapWritable>create());
    MapElements<LinkedMapWritable, String> transformFunc =
        MapElements.<LinkedMapWritable, String>via(new SimpleFunction<LinkedMapWritable, String>() {
          @Override
          public String apply(LinkedMapWritable mapw) {
            String rowValue = "";
            rowValue = convertMapWRowToString(mapw, rowValue);
            return rowValue;
          }
        });
    PCollection<String> textValues = values.apply(transformFunc);
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();
  }

  /**
   * Returns Hadoop configuration for reading data from Elasticsearch. Configuration object should
   * have InputFormat class, key class and value class to be set. Mandatory fields for ESInputFormat
   * to be set are es.resource, es.nodes, es.port, es.internal.es.version, es.nodes.wan.only. Please
   * refer <a href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
  private static Configuration getConfiguration(HIFTestOptions options) {
    Configuration conf = new Configuration();
    conf.set(ConfigurationOptions.ES_NODES, options.getServerIp());
    conf.set(ConfigurationOptions.ES_PORT, options.getServerPort().toString());
    conf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, TRUE);
    // Set username and password if Elasticsearch is configured with security.
    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, options.getUserName());
    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, options.getPassword());
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, Text.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, LinkedMapWritable.class, Object.class);
    return conf;
  }
}
