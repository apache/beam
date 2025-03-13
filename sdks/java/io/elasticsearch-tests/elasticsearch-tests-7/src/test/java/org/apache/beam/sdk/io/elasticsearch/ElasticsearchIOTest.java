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
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.getEsIndex;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.createConnectionConfig;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.createIndex;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.createTestContainer;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.deleteIndex;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.setDefaultTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.testing.TestPipeline;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

/** Tests for {@link ElasticsearchIO} version 7. */
public class ElasticsearchIOTest implements Serializable {

  private ElasticsearchIOTestCommon elasticsearchIOTestCommon;
  private ConnectionConfiguration connectionConfiguration;
  private static ElasticsearchContainer container;
  private static RestClient client;
  static final String IMAGE_TAG = "7.9.2";

  @BeforeClass
  public static void beforeClass() throws IOException {
    // Create the elasticsearch container.
    container = createTestContainer(IMAGE_TAG);

    // Start the container. This step might take some time...
    container.start();
    client = ElasticsearchIOTestUtils.clientFromContainer(container, true);
    setDefaultTemplate(client);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    client.close();
    container.stop();
  }

  @Before
  public void setup() throws IOException {
    if (connectionConfiguration == null) {
      connectionConfiguration = createConnectionConfig(client);
      elasticsearchIOTestCommon =
          new ElasticsearchIOTestCommon(connectionConfiguration, client, false);

      deleteIndex(client, getEsIndex());
    }
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSizes() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(elasticsearchIOTestCommon.restClient, getEsIndex());
    elasticsearchIOTestCommon.testSizes();
  }

  @Test
  public void testSizesWithAlias() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(elasticsearchIOTestCommon.restClient, getEsIndex(), true);
    elasticsearchIOTestCommon.testSizes();
  }

  @Test
  public void testRead() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(elasticsearchIOTestCommon.restClient, getEsIndex());
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testRead();
  }

  @Test
  public void testReadWithQueryString() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(elasticsearchIOTestCommon.restClient, getEsIndex());
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testRead();
  }

  @Test
  public void testReadWithQueryValueProvider() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(elasticsearchIOTestCommon.restClient, getEsIndex());
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithQueryValueProvider();
  }

  @Test
  public void testWrite() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWrite();
  }

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testWriteWithErrors() throws Exception {
    elasticsearchIOTestCommon.setExpectedException(expectedException);
    elasticsearchIOTestCommon.testWriteWithErrors();
  }

  @Test
  public void testWriteWithErrorsReturned() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithErrorsReturned();
  }

  @Test
  public void testWriteWithErrorsReturnedAllowedErrors() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithErrorsReturnedAllowedErrors();
  }

  @Test
  public void testWriteWithMaxBatchSize() throws Exception {
    elasticsearchIOTestCommon.testWriteWithMaxBatchSize();
  }

  @Test
  public void testWriteWithMaxBatchSizeBytes() throws Exception {
    elasticsearchIOTestCommon.testWriteWithMaxBatchSizeBytes();
  }

  @Test
  public void testSplit() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(elasticsearchIOTestCommon.restClient, getEsIndex());
    elasticsearchIOTestCommon.testSplit(2_000);
  }

  @Test
  public void testWriteWithIdFn() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithIdFn();
  }

  @Test
  public void testWriteWithIndexFn() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithIndexFn();
  }

  @Test
  public void testWriteFullAddressing() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithFullAddressing();
  }

  @Test
  public void testWritePartialUpdate() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWritePartialUpdate();
  }

  @Test
  public void testWriteAppendOnly() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteAppendOnly();
  }

  @Test(expected = Exception.class)
  public void testWriteAppendOnlyDeleteNotAllowed() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteAppendOnlyDeleteNotAllowed();
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

  @Test
  public void testMaxParallelRequestsPerWindow() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testMaxParallelRequestsPerWindow();
  }

  @Test
  public void testReadWithMetadata() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithMetadata();
  }

  @Test
  public void testDefaultRetryPredicate() throws IOException {
    elasticsearchIOTestCommon.testDefaultRetryPredicate(client);
  }

  @Test
  public void testWriteRetry() throws Throwable {
    elasticsearchIOTestCommon.setExpectedException(expectedException);
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteRetry();
  }

  @Test
  public void testWriteRetryValidRequest() throws Throwable {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteRetryValidRequest();
  }

  @Test
  public void testWriteWithIsDeleteFn() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithIsDeletedFnWithPartialUpdates();
    elasticsearchIOTestCommon.testWriteWithIsDeletedFnWithoutPartialUpdate();
  }

  @Test
  public void testDocToBulkAndBulkIO() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testDocToBulkAndBulkIO();
  }

  @Test
  public void testDocumentCoder() throws Exception {
    elasticsearchIOTestCommon.testDocumentCoder();
  }

  @Test
  public void testPDone() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testPipelineDone();
  }

  @Test
  public void testValidSSLAndUsernameConfiguration() throws Exception {
    URL fileUrl = getClass().getClassLoader().getResource("clientkeystore");
    Path filePath = Paths.get(fileUrl.toURI());
    elasticsearchIOTestCommon.testValidSSLAndUsernameConfiguration(
        filePath.toAbsolutePath().toString());
  }

  @Test
  public void testWriteWindowPreservation() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWindowPreservation();
  }
}
