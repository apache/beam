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

import static org.apache.beam.sdk.io.solr.SolrIO.RetryConfiguration.DEFAULT_RETRY_PREDICATE;
import static org.apache.beam.sdk.io.solr.SolrIOTestUtils.namedThreadIsAlive;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableSet;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.solr.SolrIOTestUtils.LenientRetryPredicate;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.security.Sha256AuthenticationProvider;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A test of {@link SolrIO} on an independent Solr instance. */
@ThreadLeakScope(value = ThreadLeakScope.Scope.NONE)
@SolrTestCaseJ4.SuppressSSL
@RunWith(RandomizedRunner.class)
public class SolrIOTest extends SolrCloudTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(SolrIOTest.class);

  private static final String SOLR_COLLECTION = "beam";
  private static final int NUM_SHARDS = 3;
  private static final long NUM_DOCS = 400L;
  private static final int NUM_SCIENTISTS = 10;
  private static final int BATCH_SIZE = 200;
  private static final int DEFAULT_BATCH_SIZE = 1000;

  private static AuthorizedSolrClient<CloudSolrClient> solrClient;
  private static SolrIO.ConnectionConfiguration connectionConfiguration;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(SolrIO.class);

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

  @Test
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
  public void testRead() throws Exception {
    SolrIOTestUtils.insertTestDocuments(SOLR_COLLECTION, NUM_DOCS, solrClient);

    PCollection<SolrDocument> output =
        pipeline.apply(
            SolrIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .from(SOLR_COLLECTION)
                .withBatchSize(101));
    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_DOCS);
    pipeline.run();
  }

  @Test
  public void testReadAll() throws Exception {
    SolrIOTestUtils.insertTestDocuments(SOLR_COLLECTION, NUM_DOCS, solrClient);

    PCollection<SolrDocument> output =
        pipeline
            .apply(
                Create.of(
                    SolrIO.read()
                        .withConnectionConfiguration(connectionConfiguration)
                        .from(SOLR_COLLECTION)
                        .withBatchSize(101)))
            .apply(SolrIO.readAll());
    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_DOCS);
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

  /**
   * Test that retries are invoked when Solr returns error. We invoke this by calling a non existing
   * collection, and use a strategy that will retry on any SolrException. The logger is used to
   * verify expected behavior.
   */
  @Test
  public void testWriteRetry() throws Throwable {
    thrown.expect(IOException.class);
    thrown.expectMessage("Error writing to Solr");

    // entry state of the release tracker to ensure we only unregister newly created objects
    @SuppressWarnings("unchecked")
    Set<Object> entryState = ImmutableSet.copyOf(ObjectReleaseTracker.OBJECTS.keySet());

    SolrIO.Write write =
        SolrIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withRetryConfiguration(
                SolrIO.RetryConfiguration.create(3, Duration.standardMinutes(3))
                    .withRetryPredicate(new LenientRetryPredicate()))
            .to("wrong-collection");

    List<SolrInputDocument> data = SolrIOTestUtils.createDocuments(NUM_DOCS);
    pipeline.apply(Create.of(data)).apply(write);

    try {
      pipeline.run();
    } catch (final Pipeline.PipelineExecutionException e) {
      // Hack: await all worker threads completing (BEAM-4040)
      int waitAttempts = 30; // defensive coding
      while (namedThreadIsAlive("direct-runner-worker") && waitAttempts-- >= 0) {
        LOG.info("Pausing to allow direct-runner-worker threads to finish");
        Thread.sleep(1000);
      }

      // remove solrClients created by us as there are no guarantees on Teardown here
      for (Object o : ObjectReleaseTracker.OBJECTS.keySet()) {
        if (o instanceof SolrZkClient && !entryState.contains(o)) {
          LOG.info("Removing unreleased SolrZkClient");
          ObjectReleaseTracker.release(o);
        }
      }

      // check 2 retries were initiated by inspecting the log before passing on the exception
      expectedLogs.verifyWarn(String.format(SolrIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 1));
      expectedLogs.verifyWarn(String.format(SolrIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 2));

      throw e.getCause();
    }

    fail("Pipeline should not have run to completion");
  }

  /** Tests predicate performs as documented. */
  @Test
  public void testDefaultRetryPredicate() {
    assertTrue(DEFAULT_RETRY_PREDICATE.test(new IOException("test")));
    assertTrue(DEFAULT_RETRY_PREDICATE.test(new SolrServerException("test")));

    assertTrue(
        DEFAULT_RETRY_PREDICATE.test(new SolrException(SolrException.ErrorCode.CONFLICT, "test")));
    assertTrue(
        DEFAULT_RETRY_PREDICATE.test(
            new SolrException(SolrException.ErrorCode.SERVER_ERROR, "test")));
    assertTrue(
        DEFAULT_RETRY_PREDICATE.test(
            new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "test")));
    assertTrue(
        DEFAULT_RETRY_PREDICATE.test(
            new SolrException(SolrException.ErrorCode.INVALID_STATE, "test")));
    assertTrue(
        DEFAULT_RETRY_PREDICATE.test(new SolrException(SolrException.ErrorCode.UNKNOWN, "test")));
    assertTrue(
        DEFAULT_RETRY_PREDICATE.test(
            new HttpSolrClient.RemoteSolrException(
                "localhost",
                SolrException.ErrorCode.SERVICE_UNAVAILABLE.code,
                "test",
                new Exception())));

    assertFalse(
        DEFAULT_RETRY_PREDICATE.test(
            new SolrException(SolrException.ErrorCode.BAD_REQUEST, "test")));
    assertFalse(
        DEFAULT_RETRY_PREDICATE.test(new SolrException(SolrException.ErrorCode.FORBIDDEN, "test")));
    assertFalse(
        DEFAULT_RETRY_PREDICATE.test(new SolrException(SolrException.ErrorCode.NOT_FOUND, "test")));
    assertFalse(
        DEFAULT_RETRY_PREDICATE.test(
            new SolrException(SolrException.ErrorCode.UNAUTHORIZED, "test")));
    assertFalse(
        DEFAULT_RETRY_PREDICATE.test(
            new SolrException(SolrException.ErrorCode.UNSUPPORTED_MEDIA_TYPE, "test")));
  }

  /** Tests batch size default and changed value. */
  @Test
  public void testBatchSize() {
    SolrIO.Write write1 =
        SolrIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    assertTrue(write1.getMaxBatchSize() == BATCH_SIZE);
    SolrIO.Write write2 = SolrIO.write().withConnectionConfiguration(connectionConfiguration);
    assertTrue(write2.getMaxBatchSize() == DEFAULT_BATCH_SIZE);
  }
}
