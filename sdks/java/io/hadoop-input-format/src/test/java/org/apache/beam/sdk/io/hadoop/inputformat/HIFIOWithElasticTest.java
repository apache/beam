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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests to validate HadoopInputFormatIO for embedded Elasticsearch instance.
 *
 * <p>{@link EsInputFormat} can be used to read data from Elasticsearch. EsInputFormat by default
 * returns key class as Text and value class as LinkedMapWritable. You can also set MapWritable as
 * value class, provided that you set the property "mapred.mapoutput.value.class" with
 * MapWritable.class. If this property is not set then, using MapWritable as value class may give
 * org.apache.beam.sdk.coders.CoderException due to unexpected extra bytes after decoding.
 */
@RunWith(JUnit4.class)
public class HIFIOWithElasticTest implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HIFIOWithElasticTest.class);
  private static final String ELASTIC_IN_MEM_HOSTNAME = "127.0.0.1";
  private static int port;
  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "beamdb";
  private static final String ELASTIC_TYPE_NAME = "scientists";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;
  private static final int TEST_DATA_ROW_COUNT = 10;
  private static final String ELASTIC_TYPE_ID_PREFIX = "s";

  @ClassRule public static TemporaryFolder elasticTempFolder = new TemporaryFolder();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startServer() throws NodeValidationException, IOException {
    port = NetworkTestHelper.getAvailableLocalPort();
    ElasticEmbeddedServer.startElasticEmbeddedServer();
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
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
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
        "{"
            + "  \"query\": {"
            + "  \"match\" : {"
            + "    \"id\" : {"
            + "      \"query\" : \""
            + fieldValue
            + "\","
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
   * fields for ESInputFormat to be set are es.resource, es.nodes, es.port, es.internal.es.version.
   * Please refer to <a
   * href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(ConfigurationOptions.ES_NODES, ELASTIC_IN_MEM_HOSTNAME);
    conf.set(ConfigurationOptions.ES_PORT, String.format("%s", port));
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, TRUE);
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

  @AfterClass
  public static void shutdownServer() throws IOException {
    ElasticEmbeddedServer.shutdown();
  }

  /** Class for in memory Elasticsearch server. */
  static class ElasticEmbeddedServer implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Node node;

    static void startElasticEmbeddedServer() throws NodeValidationException {
      Settings settings =
          Settings.builder()
              .put("node.data", TRUE)
              .put("network.host", ELASTIC_IN_MEM_HOSTNAME)
              .put("http.port", port)
              .put("path.data", elasticTempFolder.getRoot().getPath())
              .put("path.home", elasticTempFolder.getRoot().getPath())
              .put("transport.type", "local")
              .put("http.enabled", TRUE)
              .put("node.ingest", TRUE)
              .build();
      node = new PluginNode(settings);
      node.start();
      LOG.info("Elastic in memory server started.");
      prepareElasticIndex();
      LOG.info(
          "Prepared index "
              + ELASTIC_INDEX_NAME
              + "and populated data on elastic in memory server.");
    }

    /** Prepares Elastic index, by adding rows. */
    private static void prepareElasticIndex() {
      CreateIndexRequest indexRequest = new CreateIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().create(indexRequest).actionGet();
      for (int i = 0; i < TEST_DATA_ROW_COUNT; i++) {
        node.client()
            .prepareIndex(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, String.valueOf(i))
            .setSource(createElasticRow(ELASTIC_TYPE_ID_PREFIX + i, "Faraday" + i))
            .execute()
            .actionGet();
      }
      node.client().admin().indices().prepareRefresh(ELASTIC_INDEX_NAME).get();
    }
    /**
     * Shutdown the embedded instance.
     *
     * @throws IOException
     */
    static void shutdown() throws IOException {
      DeleteIndexRequest indexRequest = new DeleteIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().delete(indexRequest).actionGet();
      LOG.info("Deleted index " + ELASTIC_INDEX_NAME + " from elastic in memory server");
      node.close();
      LOG.info("Closed elastic in memory server node.");
      deleteElasticDataDirectory();
    }

    private static void deleteElasticDataDirectory() {
      try {
        FileUtils.deleteDirectory(new File(elasticTempFolder.getRoot().getPath()));
      } catch (IOException e) {
        throw new RuntimeException("Could not delete elastic data directory: " + e.getMessage(), e);
      }
    }
  }

  /** Class created for handling "http.enabled" property as "true" for Elasticsearch node. */
  static class PluginNode extends Node implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ImmutableList<Class<? extends Plugin>> PLUGINS =
        ImmutableList.of(Netty4Plugin.class);

    PluginNode(final Settings settings) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null), PLUGINS);
    }
  }
}
