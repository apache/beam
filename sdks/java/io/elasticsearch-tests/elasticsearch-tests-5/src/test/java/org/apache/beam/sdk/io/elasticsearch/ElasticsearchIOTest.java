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
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_TYPE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.getEsIndex;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.testing.TestPipeline;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/*
Cannot use @RunWith(JUnit4.class) with ESIntegTestCase
Cannot have @BeforeClass @AfterClass with ESIntegTestCase
*/

/** Tests for {@link ElasticsearchIO} version 5. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
// use cluster of 1 node that has data + master roles
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, supportsDedicatedMasters = false)
public class ElasticsearchIOTest extends ESIntegTestCase implements Serializable {

  private ElasticsearchIOTestCommon elasticsearchIOTestCommon;
  private ConnectionConfiguration connectionConfiguration;

  private String[] fillAddresses() {
    ArrayList<String> result = new ArrayList<>();
    for (InetSocketAddress address : cluster().httpAddresses()) {
      result.add(String.format("http://%s:%s", address.getHostString(), address.getPort()));
    }
    return result.toArray(new String[result.size()]);
  }

  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    System.setProperty("es.set.netty.runtime.available.processors", "false");
    return Settings.builder()
        .put(super.nodeSettings(nodeOrdinal))
        .put("http.enabled", "true")
        // had problems with some jdk, embedded ES was too slow for bulk insertion,
        // and queue of 50 was full. No pb with real ES instance (cf testWrite integration test)
        .put("thread_pool.bulk.queue_size", 400)
        .build();
  }

  @Override
  public Settings indexSettings() {
    return Settings.builder()
        .put(super.indexSettings())
        // useful to have updated sizes for getEstimatedSize
        .put("index.store.stats_refresh_interval", 0)
        .build();
  }

  @Override
  protected Collection<Class<? extends Plugin>> nodePlugins() {
    ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
    plugins.add(Netty4Plugin.class);
    return plugins;
  }

  @Before
  public void setup() throws IOException {
    if (connectionConfiguration == null) {
      connectionConfiguration =
          ConnectionConfiguration.create(fillAddresses(), getEsIndex(), ES_TYPE)
              .withSocketTimeout(120000)
              .withConnectTimeout(5000);
      elasticsearchIOTestCommon =
          new ElasticsearchIOTestCommon(connectionConfiguration, getRestClient(), false);
    }
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSizes() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(getEsIndex());
    elasticsearchIOTestCommon.testSizes();
  }

  @Test
  public void testRead() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(getEsIndex());
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testRead();
  }

  @Test
  public void testReadWithQueryString() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(getEsIndex());
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithQueryString();
  }

  @Test
  public void testReadWithQueryValueProvider() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(getEsIndex());
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
    createIndex(getEsIndex());
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
  public void testWriteWithTypeFn() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithTypeFn2x5x();
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
  public void testReadWithMetadata() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithMetadata();
  }

  @Test
  public void testDefaultRetryPredicate() throws IOException {
    elasticsearchIOTestCommon.testDefaultRetryPredicate(getRestClient());
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
  public void testInvalidWriteWithIsDeleteFn() throws Throwable {
    elasticsearchIOTestCommon.setExpectedException(expectedException);
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testInvalidWriteWithIsDeletedFn();
  }
}
