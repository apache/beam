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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTest.ES_INDEX;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTest.ES_TYPE;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for Elasticsearch IO.
 */
public class ElasticsearchIOIT {
  private static final String ES_IP = "localhost";
  private static final String ES_HTTP_PORT = "9200";
  public static final ElasticsearchIO.ConnectionConfiguration CONNECTION_CONFIGURATION =
      ElasticsearchIO.ConnectionConfiguration.create(new String[]{"http://" + ES_IP + ":" + ES_HTTP_PORT}, ES_INDEX, ES_TYPE);
  public static final long NBDOCS_INSERTED = 60000;
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIOTest.class);


  @Test
  public void testSplitsVolume() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(CONNECTION_CONFIGURATION);
    ElasticsearchIO.BoundedElasticsearchSource initialSource =
        new ElasticsearchIO.BoundedElasticsearchSource(read, null);
    long desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<String>> splits = initialSource.splitIntoBundles(
        desiredBundleSizeBytes, options);
    SourceTestUtils.
        assertSourcesEqualReferenceSource(initialSource, splits, options);
    long expectedNbSplits = 5;
    assertEquals(expectedNbSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals(expectedNbSplits, nonEmptySplits);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadVolume() throws Exception {
    //use flink runner otherwise only one source is created
    String[] args = new String[] { "--runner=FlinkRunner", "--project=test-project" };
    TestPipeline pipeline =
        TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());
    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withConnectionConfiguration(CONNECTION_CONFIGURATION).withScrollKeepalive("5m"));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NBDOCS_INSERTED);
    pipeline.run();
  }

  @Test
  public void testEstimatedSizesVolume() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(CONNECTION_CONFIGURATION);
    ElasticsearchIO.BoundedElasticsearchSource initialSource =
        new ElasticsearchIO.BoundedElasticsearchSource(read, null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOGGER.info("Estimated size: {}", estimatedSize);
    assertTrue("Wrong estimated size", estimatedSize > 1500000);
  }

}
