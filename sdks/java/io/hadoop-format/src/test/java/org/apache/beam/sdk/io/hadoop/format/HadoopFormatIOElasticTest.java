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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests to validate HadoopFormatIO for Elasticsearch container.
 *
 * <p>{@link EsInputFormat} can be used to read data from Elasticsearch. EsInputFormat by default
 * returns key class as Text and value class as LinkedMapWritable. You can also set MapWritable as
 * value class, provided that you set the property "mapred.mapoutput.value.class" with
 * MapWritable.class. If this property is not set then, using MapWritable as value class may give
 * org.apache.beam.sdk.coders.CoderException due to unexpected extra bytes after decoding.
 */
@RunWith(JUnit4.class)
public class HadoopFormatIOElasticTest implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "beamdb";
  private static final int TEST_DATA_ROW_COUNT = 10;
  private static final String ELASTIC_TYPE_ID_PREFIX = "s";
  private static final String ES_VERSION = "7.9.2";

  @ClassRule public static TemporaryFolder elasticTempFolder = new TemporaryFolder();

  @ClassRule
  public static ElasticsearchContainer elasticsearch =
      new ElasticsearchContainer(
          DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
              .withTag(ES_VERSION));

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startServer() throws IOException {
    prepareElasticIndex();
  }

  /**
   * Test to read data from embedded Elasticsearch instance and verify whether data is read
   * successfully.
   */
  @Test
  public void testHifIOWithElastic() {
    // Expected hashcode is evaluated during insertion time one time and hardcoded here.
    String expectedHashCode = "a62a85f5f081e3840baf1028d4d6c6bc";
    Configuration conf = getConfiguration();
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.globally());
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PAssert.thatSingleton(count).isEqualTo((long) TEST_DATA_ROW_COUNT);
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
              return mapw.get(new Text("id")) + "|" + mapw.get(new Text("scientist"));
            }
          });
  /**
   * Test to read data from embedded Elasticsearch instance based on query and verify whether data
   * is read successfully.
   */
  @Test
  public void testHifIOWithElasticQuery() {
    long expectedRowCount = 1L;
    String expectedHashCode = "cfbf3e5c993d44e57535a114e25f782d";
    Configuration conf = getConfiguration();
    String fieldValue = ELASTIC_TYPE_ID_PREFIX + "2";
    String query =
        String.format("{\"query\": { \"match\": { \"id\": { \"query\": \"%s\" }}}}", fieldValue);
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.globally());
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PAssert.thatSingleton(count).isEqualTo(expectedRowCount);
    PCollection<LinkedMapWritable> values = esData.apply(Values.create());
    PCollection<String> textValues = values.apply(transformFunc);
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();
  }

  /**
   * Set the Elasticsearch configuration parameters in the Hadoop configuration object.
   * Configuration object should have InputFormat class, key class and value class set. Mandatory
   * fields for ESInputFormat to be set are es.resource, es.nodes, es.port, es.nodes.wan.only.
   * Please refer to <a
   * href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(ConfigurationOptions.ES_PORT, elasticsearch.getMappedPort(9200).toString());
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_INDEX_NAME);
    conf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, TRUE);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass("mapreduce.job.inputformat.class", EsInputFormat.class, InputFormat.class);
    conf.setClass("key.class", Text.class, Object.class);
    conf.setClass("value.class", LinkedMapWritable.class, Object.class);
    return conf;
  }

  private static Map<String, String> createElasticRow(String id, String name) {
    Map<String, String> data = new HashMap<>();
    data.put("id", id);
    data.put("scientist", name);
    return data;
  }

  private static void prepareElasticIndex() throws IOException {
    RestHighLevelClient client =
        new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(
                    elasticsearch.getContainerIpAddress(),
                    elasticsearch.getMappedPort(9200),
                    "http")));

    for (int i = 0; i < TEST_DATA_ROW_COUNT; i++) {
      IndexRequest request =
          new IndexRequest(ELASTIC_INDEX_NAME)
              .source(createElasticRow(ELASTIC_TYPE_ID_PREFIX + i, "Faraday" + i));
      client.index(request, RequestOptions.DEFAULT);
    }
  }
}
