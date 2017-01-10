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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithElasticTest implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final String ELASTIC_IN_MEM_HOSTNAME = "127.0.0.1";
  private static final String ELASTIC_IN_MEM_PORT = "9200";
  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "xyz";
  private static final String ELASTIC_TYPE_NAME = "employee";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;

  @BeforeClass
  public static void startServer()
      throws NodeValidationException, InterruptedException, IOException {
    ElasticEmbeddedServer.startElasticEmbeddedServer();
  }

  @Test
  public void testHifIOWithElastic() {
    TestPipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
    PCollection<KV<Text, MapWritable>> esData =
        p.apply(HadoopInputFormatIO.<Text, MapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, MapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 10);

    p.run().waitUntilFinish();
  }

  @Test
  public void testHifIOWithElasticQuery() {
    TestPipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
    String query = "{\n" + "  \"query\": {\n" + "  \"match\" : {\n" + "    \"empid\" : {\n"
        + "      \"query\" : \"xyz22602\",\n" + "      \"type\" : \"boolean\"\n" + "    }\n"
        + "  }\n" + "  }\n" + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, MapWritable>> esData =
        p.apply(HadoopInputFormatIO.<Text, MapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, MapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 1);

    p.run().waitUntilFinish();
  }

  public static Map<String, Object> populateElasticData(String empid, String name, Date joiningDate,
      String[] skills, String designation) {
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("empid", empid);
    data.put("name", name);
    data.put("joiningDate", joiningDate);
    data.put("skills", skills);
    data.put("designation", designation);
    return data;
  }

  @AfterClass
  public static void shutdownServer() throws IOException {
    ElasticEmbeddedServer.shutdown();
  }

  /**
   * Class for in memory elastic server
   *
   */
  static class ElasticEmbeddedServer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static Node node;
    private static final String DEFAULT_PATH = "target/ESData";

    public static void startElasticEmbeddedServer()
        throws UnknownHostException, NodeValidationException {

      Settings settings =
          Settings.builder().put("node.data", TRUE).put("network.host", ELASTIC_IN_MEM_HOSTNAME)
              .put("http.port", ELASTIC_IN_MEM_PORT).put("path.data", DEFAULT_PATH)
              .put("path.home", DEFAULT_PATH).put("transport.type", "local")
              .put("http.enabled", TRUE).put("node.ingest", TRUE).build();
      node = new PluginNode(settings);
      node.start();
      prepareElasticIndex();

    }

    private static void prepareElasticIndex() {
      CreateIndexRequest indexRequest = new CreateIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().create(indexRequest).actionGet();
      for (int i = 0; i < 10; i++) {
        node.client().prepareIndex(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, String.valueOf(i))
            .setSource(populateElasticData("xyz2260" + i, "John Foo", new Date(),
                new String[] {"java"}, "Software engineer"))
            .execute();
      }
      GetResponse response = node.client().prepareGet(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, "1")
          .execute().actionGet();

    }

    public Client getClient() throws UnknownHostException {
      return node.client();
    }

    public static void shutdown() throws IOException {
      DeleteIndexRequest indexRequest = new DeleteIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().delete(indexRequest).actionGet();
      node.close();
      deleteElasticDataDirectory();
    }

    private static void deleteElasticDataDirectory() {
      try {
        FileUtils.deleteDirectory(new File(DEFAULT_PATH));
      } catch (IOException e) {
        throw new RuntimeException("Exception: Could not delete elastic data directory", e);
      }
    }

  }

  /**
   *
   * Class created for handling "http.enabled" property as "true" for elastic search node
   *
   */
  static class PluginNode extends Node implements Serializable {

    private static final long serialVersionUID = 1L;
    static Collection<Class<? extends Plugin>> list = new ArrayList<Class<? extends Plugin>>();
    static {
      list.add(Netty4Plugin.class);
    }

    public PluginNode(final Settings settings) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null), list);

    }
  }

  public Configuration getConfiguration() {
    Configuration conf = new Configuration();

    conf.set(ConfigurationOptions.ES_NODES, ELASTIC_IN_MEM_HOSTNAME);
    conf.set(ConfigurationOptions.ES_PORT, String.format("%s", ELASTIC_IN_MEM_PORT));
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, TRUE);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass("mapreduce.job.inputformat.class",
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass("key.class", Text.class, Object.class);
    conf.setClass("value.class", MapWritable.class, Object.class);
    conf.setClass("mapred.mapoutput.value.class", MapWritable.class, Object.class);
    return conf;
  }
}
