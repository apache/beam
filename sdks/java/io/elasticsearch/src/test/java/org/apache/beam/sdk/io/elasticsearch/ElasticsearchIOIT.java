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

import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test of {@link ElasticsearchIO} on an independent Elasticsearch instance.
 *
 * <p>This test requires a running instance of Elasticsearch, and the test dataset must exist in the
 * database.
 *
 * <p>You can run this test by doing the following from the beam parent module directory:
 *
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/elasticsearch -DintegrationTestPipelineOptions='[
 *  "--elasticsearchServer=1.2.3.4",
 *  "--elasticsearchHttpPort=9200",
 *  "--elasticsearchTcpPort=9300" ]'
 * </pre>
 */
public class ElasticsearchIOIT {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIOIT.class);
  private static  RestClient restClient;
  private static IOTestPipelineOptions options;
  private static ElasticsearchIO.ConnectionConfiguration readConnectionConfiguration;
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(IOTestPipelineOptions.class);
    readConnectionConfiguration =
        ElasticsearchTestDataSet.getConnectionConfiguration(
            options, ElasticsearchTestDataSet.ReadOrWrite.READ);
    restClient = readConnectionConfiguration.createClient();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ElasticsearchTestDataSet.deleteIndex(restClient, ElasticsearchTestDataSet.ReadOrWrite.WRITE);
    restClient.close();
  }

  @Test
  public void testSplitsVolume() throws Exception {
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(readConnectionConfiguration);
    ElasticsearchIO.BoundedElasticsearchSource initialSource =
        new ElasticsearchIO.BoundedElasticsearchSource(read, null);
    //desiredBundleSize is ignored because in ES 2.x there is no way to split shards. So we get
    // as many bundles as ES shards and bundle size is shard size
    long desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    //this is the number of ES shards
    // (By default, each index in Elasticsearch is allocated 5 primary shards)
    long expectedNumSplits = 5;
    assertEquals(expectedNumSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals(expectedNumSplits, nonEmptySplits);
  }

  @Test
  public void testReadVolume() throws Exception {
    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read().withConnectionConfiguration(readConnectionConfiguration));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(ElasticsearchTestDataSet.NUM_DOCS);
    pipeline.run();
  }

  @Test
  public void testWriteVolume() throws Exception {
    ElasticsearchIO.ConnectionConfiguration writeConnectionConfiguration =
        ElasticsearchTestDataSet.getConnectionConfiguration(
            options, ElasticsearchTestDataSet.ReadOrWrite.WRITE);
    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            ElasticsearchTestDataSet.NUM_DOCS,
            ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(ElasticsearchIO.write().withConnectionConfiguration(writeConnectionConfiguration));
    pipeline.run();

    long currentNumDocs =
        ElasticSearchIOTestUtils.refreshIndexAndGetCurrentNumDocs(
            ElasticsearchTestDataSet.ES_INDEX, ElasticsearchTestDataSet.ES_TYPE, restClient);
    assertEquals(ElasticsearchTestDataSet.NUM_DOCS, currentNumDocs);
  }

  @Test
  public void testEstimatedSizesVolume() throws Exception {
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(readConnectionConfiguration);
    ElasticsearchIO.BoundedElasticsearchSource initialSource =
        new ElasticsearchIO.BoundedElasticsearchSource(read, null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOG.info("Estimated size: {}", estimatedSize);
    assertThat(
        "Wrong estimated size bellow minimum",
        estimatedSize,
        greaterThan(ElasticsearchTestDataSet.AVERAGE_DOC_SIZE * ElasticsearchTestDataSet.NUM_DOCS));
    assertThat(
        "Wrong estimated size beyond maximum",
        estimatedSize,
        greaterThan(ElasticsearchTestDataSet.MAX_DOC_SIZE * ElasticsearchTestDataSet.NUM_DOCS));
  }
}
