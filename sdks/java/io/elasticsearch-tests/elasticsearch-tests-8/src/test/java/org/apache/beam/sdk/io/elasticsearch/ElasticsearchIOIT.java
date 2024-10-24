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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link ElasticsearchIO} on an independent Elasticsearch v8.x instance.
 *
 * <p>This test requires a running instance of Elasticsearch, and the test dataset must exist in the
 * database. See {@link ElasticsearchIOITCommon} for instructions to achieve this.
 *
 * <p>You can run this test by doing the following from the beam parent module directory with the
 * correct server IP:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/elasticsearch-tests/elasticsearch-tests-8
 *  -DintegrationTestPipelineOptions='[
 *  "--elasticsearchServer=1.2.3.4",
 *  "--elasticsearchHttpPort=9200"]'
 *  --tests org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>It is likely that you will need to configure <code>thread_pool.write.queue_size: 250</code>
 * (or higher) in the backend Elasticsearch server for this test to run.
 */
@RunWith(JUnit4.class)
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
    ElasticsearchIOTestUtils.deleteIndex(writeConnectionConfiguration, restClient);
    ElasticsearchIOTestUtils.deleteIndex(updateConnectionConfiguration, restClient);
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
  public void testReadPITVolume() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadPIT();
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
  public void testWriteVolumeStateful() throws Exception {
    // cannot share elasticsearchIOTestCommon because tests run in parallel.
    ElasticsearchIOTestCommon elasticsearchIOTestCommonWrite =
        new ElasticsearchIOTestCommon(writeConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonWrite.setPipeline(pipeline);
    elasticsearchIOTestCommonWrite.testWriteStateful();
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

  @Test
  public void testWriteWithAllowableErrors() throws Exception {
    elasticsearchIOTestCommon.testWriteWithAllowedErrors();
  }

  @Test
  public void testWriteWithRouting() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithRouting();
  }

  @Test
  public void testWriteScriptedUpsert() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteScriptedUpsert();
  }

  @Test
  public void testWriteWithDocVersion() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithDocVersion();
  }

  /**
   * This test verifies volume partial updates of Elasticsearch. The test dataset index is cloned
   * and then a new field is added to each document using a partial update. The test then asserts
   * the updates were applied.
   */
  @Test
  public void testWritePartialUpdate() throws Exception {
    ElasticsearchIOTestUtils.copyIndex(
        restClient,
        readConnectionConfiguration.getIndex(),
        updateConnectionConfiguration.getIndex());
    // cannot share elasticsearchIOTestCommon because tests run in parallel.
    ElasticsearchIOTestCommon elasticsearchIOTestCommonUpdate =
        new ElasticsearchIOTestCommon(updateConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonUpdate.setPipeline(pipeline);
    elasticsearchIOTestCommonUpdate.testWritePartialUpdate();
  }

  /**
   * This test verifies volume deletes of Elasticsearch. The test dataset index is cloned and then
   * around half of the documents are deleted and the other half is partially updated using bulk
   * delete request. The test then asserts the documents were deleted successfully.
   */
  @Test
  public void testWriteWithIsDeletedFnWithPartialUpdates() throws Exception {
    ElasticsearchIOTestUtils.copyIndex(
        restClient,
        readConnectionConfiguration.getIndex(),
        updateConnectionConfiguration.getIndex());
    ElasticsearchIOTestCommon elasticsearchIOTestCommonDeleteFn =
        new ElasticsearchIOTestCommon(updateConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonDeleteFn.setPipeline(pipeline);
    elasticsearchIOTestCommonDeleteFn.testWriteWithIsDeletedFnWithPartialUpdates();
  }

  /**
   * This test verifies volume deletes of Elasticsearch. The test dataset index is cloned and then
   * around half of the documents are deleted using bulk delete request. The test then asserts the
   * documents were deleted successfully.
   */
  @Test
  public void testWriteWithIsDeletedFnWithoutPartialUpdate() throws Exception {
    ElasticsearchIOTestUtils.copyIndex(
        restClient,
        readConnectionConfiguration.getIndex(),
        updateConnectionConfiguration.getIndex());
    ElasticsearchIOTestCommon elasticsearchIOTestCommonDeleteFn =
        new ElasticsearchIOTestCommon(updateConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonDeleteFn.setPipeline(pipeline);
    elasticsearchIOTestCommonDeleteFn.testWriteWithIsDeletedFnWithoutPartialUpdate();
  }
}
