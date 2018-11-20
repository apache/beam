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
package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;

import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOITCommon.ElasticsearchPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * A test of {@link ElasticsearchIO} on an independent Elasticsearch v5.x instance.
 *
 * <p>This test requires a running instance of Elasticsearch, and the test dataset must exist in the
 * database. See {@link ElasticsearchIOITCommon} for instructions to achieve this.
 *
 * <p>You can run this test by doing the following from the beam parent module directory with the
 * correct server IP:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/elasticsearch-tests/elasticsearch-tests-5
 *  -DintegrationTestPipelineOptions='[
 *  "--elasticsearchServer=1.2.3.4",
 *  "--elasticsearchHttpPort=9200"]'
 *  --tests org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>It is likely that you will need to configure <code>thread_pool.bulk.queue_size: 250</code> (or
 * higher) in the backend Elasticsearch server for this test to run.
 */
public class ElasticsearchIOIT {
  private static RestClient restClient;
  private static ElasticsearchPipelineOptions options;
  private static ConnectionConfiguration readConnectionConfiguration;
  private static ConnectionConfiguration writeConnectionConfiguration;
  private static ConnectionConfiguration updateConnectionConfiguration;
  private static ElasticsearchIOTestCommon elasticsearchIOTestCommon;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    PipelineOptionsFactory.register(ElasticsearchPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(ElasticsearchPipelineOptions.class);
    readConnectionConfiguration =
        ElasticsearchIOITCommon.getConnectionConfiguration(
            options, ElasticsearchIOITCommon.IndexMode.READ);
    writeConnectionConfiguration =
        ElasticsearchIOITCommon.getConnectionConfiguration(
            options, ElasticsearchIOITCommon.IndexMode.WRITE);
    updateConnectionConfiguration =
        ElasticsearchIOITCommon.getConnectionConfiguration(
            options, ElasticsearchIOITCommon.IndexMode.WRITE_PARTIAL);
    restClient = readConnectionConfiguration.createClient();
    elasticsearchIOTestCommon =
        new ElasticsearchIOTestCommon(readConnectionConfiguration, restClient, true);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ElasticSearchIOTestUtils.deleteIndex(writeConnectionConfiguration, restClient);
    ElasticSearchIOTestUtils.deleteIndex(updateConnectionConfiguration, restClient);
    restClient.close();
  }

  @Test
  public void testSplitsVolume() throws Exception {
    elasticsearchIOTestCommon.testSplit(10_000);
  }

  @Test
  public void testReadVolume() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testRead();
  }

  @Test
  public void testWriteVolume() throws Exception {
    // cannot share elasticsearchIOTestCommon because tests run in parallel.
    ElasticsearchIOTestCommon elasticsearchIOTestCommonWrite =
        new ElasticsearchIOTestCommon(writeConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonWrite.setPipeline(pipeline);
    elasticsearchIOTestCommonWrite.testWrite();
  }

  @Test
  public void testSizesVolume() throws Exception {
    elasticsearchIOTestCommon.testSizes();
  }

  /**
   * This test verifies volume loading of Elasticsearch using explicit document IDs and routed to an
   * index named the same as the scientist, and type which is based on the modulo 2 of the scientist
   * name. The goal of this IT is to help observe and verify that the overhead of adding the
   * functions to parse the document and extract the ID is acceptable.
   */
  @Test
  public void testWriteWithFullAddressingVolume() throws Exception {
    // cannot share elasticsearchIOTestCommon because tests run in parallel.
    ElasticsearchIOTestCommon elasticsearchIOTestCommonWrite =
        new ElasticsearchIOTestCommon(writeConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonWrite.setPipeline(pipeline);
    elasticsearchIOTestCommonWrite.testWriteWithFullAddressing();
  }

  /**
   * This test verifies volume partial updates of Elasticsearch. The test dataset index is cloned
   * and then a new field is added to each document using a partial update. The test then asserts
   * the updates where appied.
   */
  @Test
  public void testWritePartialUpdate() throws Exception {
    ElasticSearchIOTestUtils.copyIndex(
        restClient,
        readConnectionConfiguration.getIndex(),
        updateConnectionConfiguration.getIndex());
    // cannot share elasticsearchIOTestCommon because tests run in parallel.
    ElasticsearchIOTestCommon elasticsearchIOTestCommonUpdate =
        new ElasticsearchIOTestCommon(updateConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonUpdate.setPipeline(pipeline);
    elasticsearchIOTestCommonUpdate.testWritePartialUpdate();
  }
}
