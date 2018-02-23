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

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BoundedElasticsearchSource;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Read;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_INDEX;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_TYPE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.NUM_DOCS_UTESTS;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.Matchers.lessThan;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
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
    return Settings.builder().put(super.nodeSettings(nodeOrdinal))
        .put("http.enabled", "true")
        // had problems with some jdk, embedded ES was too slow for bulk insertion,
        // and queue of 50 was full. No pb with real ES instance (cf testWrite integration test)
        .put("thread_pool.bulk.queue_size", 100)
        .build();
  }

  @Override
  public Settings indexSettings() {
    return Settings.builder().put(super.indexSettings())
        //useful to have updated sizes for getEstimatedSize
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
  public void setup() {
    if (connectionConfiguration == null) {
      connectionConfiguration = ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
      elasticsearchIOTestCommon = new ElasticsearchIOTestCommon(connectionConfiguration,
          getRestClient(), false);
    }
  }
  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSizes() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(ES_INDEX);
    elasticsearchIOTestCommon.testSizes();
  }

  @Test
  public void testRead() throws Exception {
    // need to create the index using the helper method (not create it at first insertion)
   // for the indexSettings() to be run
   createIndex(ES_INDEX);
   elasticsearchIOTestCommon.setPipeline(pipeline);
   elasticsearchIOTestCommon.testRead();
 }

  @Test
  public void testReadWithQuery() throws Exception {
   // need to create the index using the helper method (not create it at first insertion)
   // for the indexSettings() to be run
   createIndex(ES_INDEX);
   elasticsearchIOTestCommon.setPipeline(pipeline);
   elasticsearchIOTestCommon.testReadWithQuery();
  }

  @Test
  public void testWrite() throws Exception {
   elasticsearchIOTestCommon.setPipeline(pipeline);
   elasticsearchIOTestCommon.testWrite();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
   //need to create the index using the helper method (not create it at first insertion)
   // for the indexSettings() to be run
   createIndex(ES_INDEX);
    ElasticSearchIOTestUtils
        .insertTestDocuments(connectionConfiguration, NUM_DOCS_UTESTS, getRestClient());
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
   BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null,
       null);
   int desiredBundleSizeBytes = 2000;
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
   long indexSize = BoundedElasticsearchSource.estimateIndexSize(connectionConfiguration);
   float expectedNumSourcesFloat = (float) indexSize / desiredBundleSizeBytes;
   int expectedNumSources = (int) Math.ceil(expectedNumSourcesFloat);
   assertEquals("Wrong number of splits", expectedNumSources, splits.size());
    int emptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).isEmpty()) {
        emptySplits += 1;
      }
    }
    assertThat(
        "There are too many empty splits, parallelism is sub-optimal",
        emptySplits,
        lessThan((int) (ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE * splits.size())));
  }
}
