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
package org.apache.beam.sdk.io.hadoop.format;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.common.HashingFn;
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
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A test of {@link org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO.Read} on an independent
 * Elasticsearch instance.
 *
 * <p>This test requires a running instance of Elasticsearch, and the test dataset must exist in the
 * database.
 *
 * <p>You can run this test by doing the following:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/hadoop-format/
 *  -Dit.test=org.apache.beam.sdk.io.hadoop.format.HadoopFormatIOElasticIT
 *  -DintegrationTestPipelineOptions='[
 *  "--elasticServerIp=1.2.3.4",
 *  "--elasticServerPort=port",
 *  "--elasticUserName=user",
 *  "--elasticPassword=mypass",
 *  "--withESContainer=false" ]'
 *  --tests org.apache.beam.sdk.io.hadoop.format.HadoopFormatIOElasticIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>If you want to run this with a runner besides directrunner, there are profiles for dataflow
 * and spark in the pom. You'll want to activate those in addition to the normal test runner
 * invocation pipeline options.
 */
@RunWith(JUnit4.class)
public class HadoopFormatIOElasticIT implements Serializable {

  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "test_data";
  private static HadoopFormatIOTestOptions options;
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static ElasticsearchContainer elasticsearch;

  @BeforeClass
  public static void setUp() throws IOException {
    PipelineOptionsFactory.register(HadoopFormatIOTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HadoopFormatIOTestOptions.class);
    if (options.isWithTestcontainers()) {
      setElasticsearchContainer();
    }
  }

  @AfterClass
  public static void afterClass() {
    if (elasticsearch != null) {
      elasticsearch.stop();
    }
  }

  /**
   * This test reads data from the Elasticsearch instance and verifies whether data is read
   * successfully.
   */
  @Test
  public void testHifIOWithElastic() throws SecurityException {
    // Expected hashcode is evaluated during insertion time one time and hardcoded here.
    final long expectedRowCount = 1000L;
    String expectedHashCode = "42e254c8689050ed0a617ff5e80ea392";
    Configuration conf = getConfiguration();
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
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

  private final MapElements<LinkedMapWritable, String> transformFunc =
      MapElements.via(
          new SimpleFunction<LinkedMapWritable, String>() {
            @Override
            public String apply(LinkedMapWritable mapw) {
              return convertMapWRowToString(mapw);
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
    Object valueObj = mapw.get(new Text(columnName));
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
    Configuration conf = getConfiguration();
    String query =
        "{"
            + "  \"query\": {"
            + "  \"match\" : {"
            + "    \"Title\" : {"
            + "      \"query\" : \"Title9\""
            + "    }"
            + "  }"
            + "  }"
            + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
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
   * to be set are es.resource, es.nodes, es.port, es.nodes.wan.only. Please refer <a
   * href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
  private static Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(ConfigurationOptions.ES_NODES, options.getElasticServerIp());
    conf.set(ConfigurationOptions.ES_PORT, options.getElasticServerPort().toString());
    conf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, TRUE);
    // Set username and password if Elasticsearch is configured with security.
    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, options.getElasticUserName());
    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, options.getElasticPassword());
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_INDEX_NAME);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass(
        "mapreduce.job.inputformat.class",
        org.elasticsearch.hadoop.mr.EsInputFormat.class,
        InputFormat.class);
    conf.setClass("key.class", Text.class, Object.class);
    conf.setClass("value.class", LinkedMapWritable.class, Object.class);
    // Optimizations added to change the max docs per partition, scroll size and batch size of
    // bytes to improve the test time for large data
    conf.set("es.input.max.docs.per.partition", "50000");
    conf.set("es.scroll.size", "400");
    conf.set("es.batch.size.bytes", "8mb");
    return conf;
  }

  private static void setElasticsearchContainer() throws IOException {
    elasticsearch =
        new ElasticsearchContainer(
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                .withTag("7.9.2"));
    elasticsearch.start();
    options.setElasticUserName("");
    options.setElasticPassword("");
    options.setElasticServerIp(elasticsearch.getContainerIpAddress());
    options.setElasticServerPort(elasticsearch.getMappedPort(9200));
    prepareElasticIndex();
  }

  private static Map<String, String> createElasticRow(Integer i) {
    Map<String, String> data = new HashMap<>();
    data.put("User_Name", "User_Name" + i);
    data.put("Item_Code", "" + i);
    data.put("Txn_ID", "" + i);
    data.put("Item_ID", "" + i);
    data.put("last_updated", "" + (i * 1000));
    data.put("Price", "" + i);
    data.put("Title", "Title" + i);
    data.put("Description", "Description" + i);
    data.put("Age", "" + i);
    data.put("Item_Name", "Item_Name" + i);
    data.put("Item_Price", "" + i);
    data.put("Availability", "" + (i % 2 == 0));
    data.put("Batch_Num", "" + i);
    data.put(
        "Last_Ordered",
        new DateTime(Instant.ofEpochSecond(i))
            .toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.000-0000")));
    data.put("City", "City" + i);
    return data;
  }

  private static void prepareElasticIndex() throws IOException {
    RestHighLevelClient client =
        new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(
                    options.getElasticServerIp(), options.getElasticServerPort(), "http")));

    for (int i = 0; i < 1000; i++) {
      IndexRequest request = new IndexRequest(ELASTIC_INDEX_NAME).source(createElasticRow(i));
      client.index(request, RequestOptions.DEFAULT);
    }
  }
}
