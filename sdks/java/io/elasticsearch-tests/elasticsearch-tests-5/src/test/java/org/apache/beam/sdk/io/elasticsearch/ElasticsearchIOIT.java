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
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
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
 * database.
 *
 * <p>You can run this test by doing the following from the beam parent module directory:
 *
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/elasticsearch -DintegrationTestPipelineOptions='[
 *  "--elasticsearchServer=1.2.3.4",
 *  "--elasticsearchHttpPort=9200"]'
 * </pre>
 */
public class ElasticsearchIOIT {
  private static RestClient restClient;
  private static IOTestPipelineOptions options;
  private static ConnectionConfiguration readConnectionConfiguration;
  private static ConnectionConfiguration writeConnectionConfiguration;
  private static ElasticsearchIOTestCommon elasticsearchIOTestCommon;

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(IOTestPipelineOptions.class);
    readConnectionConfiguration = ElasticsearchIOITCommon
        .getConnectionConfiguration(options, ElasticsearchIOITCommon.ReadOrWrite.READ);
    writeConnectionConfiguration = ElasticsearchIOITCommon
        .getConnectionConfiguration(options, ElasticsearchIOITCommon.ReadOrWrite.WRITE);
    restClient = readConnectionConfiguration.createClient();
    elasticsearchIOTestCommon = new ElasticsearchIOTestCommon(readConnectionConfiguration,
        restClient, true);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ElasticSearchIOTestUtils.deleteIndex(writeConnectionConfiguration, restClient);
    restClient.close();
  }

  @Test
  public void testSplitsVolume() throws Exception {
    Read read = ElasticsearchIO.read().withConnectionConfiguration(readConnectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null,
        null);
    int desiredBundleSizeBytes = 10000;
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    long indexSize = BoundedElasticsearchSource.estimateIndexSize(readConnectionConfiguration);
    float expectedNumSourcesFloat = (float) indexSize / desiredBundleSizeBytes;
    int expectedNumSources = (int) Math.ceil(expectedNumSourcesFloat);
    assertEquals(expectedNumSources, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals("Wrong number of empty splits", expectedNumSources, nonEmptySplits);
  }

  @Test
  public void testReadVolume() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testRead();
  }

  @Test
  public void testWriteVolume() throws Exception {
    //cannot share elasticsearchIOTestCommon because tests run in parallel.
    ElasticsearchIOTestCommon elasticsearchIOTestCommonWrite = new ElasticsearchIOTestCommon(
        writeConnectionConfiguration, restClient, true);
    elasticsearchIOTestCommonWrite.setPipeline(pipeline);
    elasticsearchIOTestCommonWrite.testWrite();
  }

  @Test
  public void testSizesVolume() throws Exception {
    elasticsearchIOTestCommon.testSizes();
  }
}
