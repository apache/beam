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
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO} on an
 * independent Elasticsearch instance.
 *
 * <p>This test requires a running instance of Elasticsearch, and the test dataset must exist in
 * the database.
 *
 * <p>You can run this test by doing the following:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/hadoop/jdk1.8-tests/HIFIOElasticIT
 *  -DintegrationTestPipelineOptions='[
 *  "--elasticServerIp=1.2.3.4",
 *  "--elasticServerPort=port",
 *  "--elasticUserName=user",
 *  "--elasticPassword=mypass" ]'
 * </pre>
 *
 * <p>If you want to run this with a runner besides directrunner, there are profiles for dataflow
 * and spark in the jdk1.8-tests pom. You'll want to activate those in addition to the normal test
 * runner invocation pipeline options.
 */

@RunWith(JUnit4.class)
public class HIFIOElasticIT implements Serializable {

  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "test_data";
  private static final String ELASTIC_TYPE_NAME = "test_type";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;
  private static HIFTestOptions options;
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
  }

  /**
   * This test reads data from the Elasticsearch instance and verifies whether data is read
   * successfully.
   */
  @Test
  public void testHifIOWithElastic() throws SecurityException, IOException {
    // Expected hashcode is evaluated during insertion time one time and hardcoded here.
    final long expectedRowCount = 1000L;
    String expectedHashCode = "42e254c8689050ed0a617ff5e80ea392";
    Configuration conf = getConfiguration(options);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PCollection<Long> count = esData.apply(Count.globally());
    PAssert.thatSingleton(count).isEqualTo(expectedRowCount);
    PCollection<LinkedMapWritable> values = esData.apply(Values.create());
    PCollection<String> textValues = values.apply(transformFunc);
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();
  }

  MapElements<LinkedMapWritable, String> transformFunc =
      MapElements.via(
          new SimpleFunction<LinkedMapWritable, String>() {
            @Override
            public String apply(LinkedMapWritable mapw) {
              String rowValue = "";
              rowValue = convertMapWRowToString(mapw);
              return rowValue;
            }
          });
  /*
   * Function to create a toString implementation of a MapWritable row by writing all field values
   * in a string row.
   */
  private String convertMapWRowToString(LinkedMapWritable mapw) {
    String rowValue = "";
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
    String expectedHashCode = "d7a7e4e42c2ca7b83ef7c1ad1ebce000";
    Long expectedRecordsCount = 1L;
    Configuration conf = getConfiguration(options);
    String query = "{"
                  + "  \"query\": {"
                  + "  \"match\" : {"
                  + "    \"Title\" : {"
                  + "      \"query\" : \"Title9\","
                  + "      \"type\" : \"boolean\""
                  + "    }"
                  + "  }"
                  + "  }"
                  + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.globally());
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PAssert.thatSingleton(count).isEqualTo(expectedRecordsCount);
    PCollection<LinkedMapWritable> values = esData.apply(Values.create());
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
    conf.set(ConfigurationOptions.ES_NODES, options.getElasticServerIp());
    conf.set(ConfigurationOptions.ES_PORT, options.getElasticServerPort().toString());
    conf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, TRUE);
    // Set username and password if Elasticsearch is configured with security.
    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, options.getElasticUserName());
    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, options.getElasticPassword());
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass("mapreduce.job.inputformat.class",
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass("key.class", Text.class, Object.class);
    conf.setClass("value.class", LinkedMapWritable.class, Object.class);
    // Optimizations added to change the max docs per partition, scroll size and batch size of
    // bytes to improve the test time for large data
    conf.set("es.input.max.docs.per.partition", "50000");
    conf.set("es.scroll.size", "400");
    conf.set("es.batch.size.bytes", "8mb");
    return conf;
  }
}
