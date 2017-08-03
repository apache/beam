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
package org.apache.beam.sdk.io.solr;

import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test of {@link SolrIO} on an independent Solr instance.
 *
 * <p>This test requires a running instance of Solr (./bin/solr start -e cloud -noprompt)
 *
 * <p>You can run this test by doing the following from the beam parent module directory:
 *
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/solr -DintegrationTestPipelineOptions='[
 *  "--solrZookeeperServer=127.0.0.1:9983"]'
 * </pre>
 */
public class SolrIOIT {

  public static final String READ_COLLECTION = "beam";
  public static final int NUM_SHARDS = 3;
  public static final long NUM_DOCS = 60000;
  public static final String WRITE_COLLECTION = "beam" + Instant.now().getMillis();
  private static final Logger LOG = LoggerFactory.getLogger(SolrIOIT.class);

  private static AuthorizedSolrClient solrClient;
  private static IOTestPipelineOptions options;
  private static SolrIO.ConnectionConfiguration connectionConfiguration;
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(IOTestPipelineOptions.class);
    connectionConfiguration = SolrIO.ConnectionConfiguration
        .create(options.getSolrZookeeperServer());
    solrClient = connectionConfiguration.createClient();

    CollectionAdminResponse listResponse =
        (CollectionAdminResponse) solrClient.process(new CollectionAdminRequest.List());
    List<String> collections = (List<String>) listResponse.getResponse().get("collections");
    if (collections.contains(READ_COLLECTION)) {
      QueryResponse response = solrClient.query(READ_COLLECTION, new SolrQuery("*:*"));
      if (response.getResults().getNumFound() != NUM_DOCS) {
        LOG.info("Collection {} is exist but the number of documents is not match, repopulate",
            READ_COLLECTION);
        SolrIOTestUtils.clearCollection(READ_COLLECTION, solrClient);
        SolrIOTestUtils.insertTestDocuments(READ_COLLECTION, NUM_DOCS, solrClient);
      }
    } else {
      LOG.info("Create and populate collection {}",
          READ_COLLECTION);
      SolrIOTestUtils.createCollection(READ_COLLECTION, NUM_SHARDS, 1, solrClient);
    }

    SolrIOTestUtils.createCollection(WRITE_COLLECTION, 2, 2, solrClient);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    SolrIOTestUtils.deleteCollection(WRITE_COLLECTION, solrClient);
    solrClient.close();
  }

  @Test
  public void testSplitsVolume() throws Exception {
    SolrIO.Read read =
            SolrIO.read().withConnectionConfiguration(connectionConfiguration);
    SolrIO.BoundedSolrSource initialSource =
        new SolrIO.BoundedSolrSource(read, null);
    //desiredBundleSize is ignored now
    long desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<SolrDocument>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    //this is the number of Solr shards
    long expectedNumSplits = NUM_SHARDS;
    assertEquals(expectedNumSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<SolrDocument> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    // docs are hashed by id to shards, in this test, NUM_DOCS >> NUM_SHARDS
    // therefore, can not exist an empty shard.
    assertEquals(expectedNumSplits, nonEmptySplits);
  }

  @Test
  public void testReadVolume() throws Exception {
    PCollection<SolrDocument> output =
        pipeline.apply(
            SolrIO.read().withConnectionConfiguration(connectionConfiguration));
    PAssert.thatSingleton(output.apply("Count", Count.<SolrDocument>globally()))
        .isEqualTo(NUM_DOCS);
    pipeline.run();
  }

  @Test
  public void testWriteVolume() throws Exception {
    List<SolrInputDocument> data = SolrIOTestUtils.createDocuments(NUM_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(SolrIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withCollection(WRITE_COLLECTION)
        );
    pipeline.run();

    long currentNumDocs = SolrIOTestUtils.commitAndGetCurrentNumDocs(WRITE_COLLECTION, solrClient);
    assertEquals(NUM_DOCS, currentNumDocs);

  }

  @Test
  public void testEstimatedSizesVolume() throws Exception {
    SolrIO.Read read =
        SolrIO.read().withConnectionConfiguration(connectionConfiguration);
    SolrIO.BoundedSolrSource initialSource =
        new SolrIO.BoundedSolrSource(read, null);
    // can't use equal assert as Solr collections never have same size
    // (due to internal Lucene implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOG.info("Estimated size: {}", estimatedSize);
    assertThat(
        "Wrong estimated size bellow minimum",
        estimatedSize,
        greaterThan(SolrIOTestUtils.MIN_DOC_SIZE * NUM_DOCS));
    assertThat(
        "Wrong estimated size beyond maximum",
        estimatedSize,
        lessThan(SolrIOTestUtils.MAX_DOC_SIZE * NUM_DOCS));
  }
}
