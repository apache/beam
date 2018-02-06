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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.security.Sha256AuthenticationProvider;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A test of {@link SolrIO} on an independent Solr instance. */
@ThreadLeakScope(value = ThreadLeakScope.Scope.NONE)
@SolrTestCaseJ4.SuppressSSL
public class SolrIOTest extends SolrCloudTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(SolrIOTest.class);

  private static final String SOLR_COLLECTION = "beam";
  private static final int NUM_SHARDS = 3;
  private static final long NUM_DOCS = 400L;
  private static final int NUM_SCIENTISTS = 10;
  private static final int BATCH_SIZE = 200;

  private static AuthorizedSolrClient<CloudSolrClient> solrClient;
  private static SolrIO.ConnectionConfiguration connectionConfiguration;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup credential for solr user,
    // See https://cwiki.apache.org/confluence/display/solr/Basic+Authentication+Plugin
    String password = "SolrRocks";
    // salt's size can be arbitrary
    byte[] salt = new byte[random().nextInt(30) + 1];
    random().nextBytes(salt);
    String base64Salt = BaseEncoding.base64().encode(salt);
    String sha56 = Sha256AuthenticationProvider.sha256(password, base64Salt);
    String credential = sha56 + " " + base64Salt;
    String securityJson =
        "{"
            + "'authentication':{"
            + "  'blockUnknown': true,"
            + "  'class':'solr.BasicAuthPlugin',"
            + "  'credentials':{'solr':'"
            + credential
            + "'}}"
            + "}";

    configureCluster(3).addConfig("conf", getFile("cloud-minimal/conf").toPath()).configure();
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    zkStateReader
        .getZkClient()
        .setData("/security.json", securityJson.getBytes(Charset.defaultCharset()), true);
    String zkAddress = cluster.getZkServer().getZkAddress();
    connectionConfiguration =
        SolrIO.ConnectionConfiguration.create(zkAddress).withBasicCredentials("solr", password);
    solrClient = connectionConfiguration.createClient();
    SolrIOTestUtils.createCollection(SOLR_COLLECTION, NUM_SHARDS, 1, solrClient);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    solrClient.close();
  }

  @Before
  public void before() throws Exception {
    SolrIOTestUtils.clearCollection(SOLR_COLLECTION, solrClient);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  public void testBadCredentials() throws IOException {
    thrown.expect(SolrException.class);

    String zkAddress = cluster.getZkServer().getZkAddress();
    SolrIO.ConnectionConfiguration connectionConfiguration =
        SolrIO.ConnectionConfiguration.create(zkAddress)
            .withBasicCredentials("solr", "wrongpassword");
    try (AuthorizedSolrClient solrClient = connectionConfiguration.createClient()) {
      SolrIOTestUtils.insertTestDocuments(SOLR_COLLECTION, NUM_DOCS, solrClient);
    }
  }

  @Test
  public void testSizes() throws Exception {
    SolrIOTestUtils.insertTestDocuments(SOLR_COLLECTION, NUM_DOCS, solrClient);

    PipelineOptions options = PipelineOptionsFactory.create();
    SolrIO.Read read =
        SolrIO.read().withConnectionConfiguration(connectionConfiguration).from(SOLR_COLLECTION);
    SolrIO.BoundedSolrSource initialSource = new SolrIO.BoundedSolrSource(read, null);
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

  @Test
  public void testRead() throws Exception {
    SolrIOTestUtils.insertTestDocuments(SOLR_COLLECTION, NUM_DOCS, solrClient);

    PCollection<SolrDocument> output =
        pipeline.apply(
            SolrIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .from(SOLR_COLLECTION)
                .withBatchSize(101));
    PAssert.thatSingleton(output.apply("Count", Count.globally()))
        .isEqualTo(NUM_DOCS);
    pipeline.run();
  }

  @Test
  public void testReadWithQuery() throws Exception {
    SolrIOTestUtils.insertTestDocuments(SOLR_COLLECTION, NUM_DOCS, solrClient);

    PCollection<SolrDocument> output =
        pipeline.apply(
            SolrIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .from(SOLR_COLLECTION)
                .withQuery("scientist:Franklin"));
    PAssert.thatSingleton(output.apply("Count", Count.globally()))
        .isEqualTo(NUM_DOCS / NUM_SCIENTISTS);
    pipeline.run();
  }

  @Test
  public void testWrite() throws Exception {
    List<SolrInputDocument> data = SolrIOTestUtils.createDocuments(NUM_DOCS);
    SolrIO.Write write =
        SolrIO.write().withConnectionConfiguration(connectionConfiguration).to(SOLR_COLLECTION);
    pipeline.apply(Create.of(data)).apply(write);
    pipeline.run();

    long currentNumDocs = SolrIOTestUtils.commitAndGetCurrentNumDocs(SOLR_COLLECTION, solrClient);
    assertEquals(NUM_DOCS, currentNumDocs);

    QueryResponse response = solrClient.query(SOLR_COLLECTION, new SolrQuery("scientist:Lovelace"));
    assertEquals(NUM_DOCS / NUM_SCIENTISTS, response.getResults().getNumFound());
  }

  @Test
  public void testWriteWithMaxBatchSize() throws Exception {
    SolrIO.Write write =
        SolrIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .to(SOLR_COLLECTION)
            .withMaxBatchSize(BATCH_SIZE);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<SolrInputDocument, Void> fnTester =
        DoFnTester.of(new SolrIO.Write.WriteFn(write))) {
      List<SolrInputDocument> input = SolrIOTestUtils.createDocuments(NUM_DOCS);
      long numDocsProcessed = 0;
      long numDocsInserted = 0;
      for (SolrInputDocument document : input) {
        fnTester.processElement(document);
        numDocsProcessed++;
        // test every 100 docs to avoid overloading Solr
        if ((numDocsProcessed % 100) == 0) {
          // force the index to upgrade after inserting for the inserted docs
          // to be searchable immediately
          long currentNumDocs =
              SolrIOTestUtils.commitAndGetCurrentNumDocs(SOLR_COLLECTION, solrClient);
          if ((numDocsProcessed % BATCH_SIZE) == 0) {
            /* bundle end */
            assertEquals(
                "we are at the end of a bundle, we should have inserted all processed documents",
                numDocsProcessed,
                currentNumDocs);
            numDocsInserted = currentNumDocs;
          } else {
            /* not bundle end */
            assertEquals(
                "we are not at the end of a bundle, we should have inserted no more documents",
                numDocsInserted,
                currentNumDocs);
          }
        }
      }
    }
  }

  @Test
  public void testSplit() throws Exception {
    SolrIOTestUtils.insertTestDocuments(SOLR_COLLECTION, NUM_DOCS, solrClient);

    PipelineOptions options = PipelineOptionsFactory.create();
    SolrIO.Read read =
        SolrIO.read().withConnectionConfiguration(connectionConfiguration).from(SOLR_COLLECTION);
    SolrIO.BoundedSolrSource initialSource = new SolrIO.BoundedSolrSource(read, null);
    //desiredBundleSize is ignored for now
    int desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<SolrDocument>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);

    int expectedNumSplits = NUM_SHARDS;
    assertEquals(expectedNumSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<SolrDocument> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    // docs are hashed by id to shards, in this test, NUM_DOCS >> NUM_SHARDS
    // therefore, can not exist an empty shard.
    assertEquals("Wrong number of empty splits", expectedNumSplits, nonEmptySplits);
  }
}
