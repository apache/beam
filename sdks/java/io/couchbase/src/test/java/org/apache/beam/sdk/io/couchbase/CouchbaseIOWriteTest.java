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
package org.apache.beam.sdk.io.couchbase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.couchbase.CouchbaseIO.ConnectionConfiguration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

/** Writes to a single-node Couchbase Server started by Testcontainers. */
@RunWith(JUnit4.class)
public class CouchbaseIOWriteTest {

  private static final String BUCKET = "beam";
  private static final String SCOPE = "app";
  private static final String COLLECTION = "airlines";

  @ClassRule
  public static final CouchbaseContainer COUCHBASE =
      new CouchbaseContainer("couchbase/server:enterprise-8.0.3")
          .withBucket(new BucketDefinition(BUCKET).withPrimaryIndex(false));

  // Independent of the pipeline's connection, so results are checked through a separate client.
  private static Cluster verifyCluster;
  private static Bucket verifyBucket;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @BeforeClass
  public static void setUpClass() {
    verifyCluster =
        Cluster.connect(
            COUCHBASE.getConnectionString(), COUCHBASE.getUsername(), COUCHBASE.getPassword());
    verifyBucket = verifyCluster.bucket(BUCKET);
    verifyBucket.waitUntilReady(Duration.ofSeconds(60));
    verifyBucket.collections().createScope(SCOPE);
    verifyBucket.collections().createCollection(SCOPE, COLLECTION);
  }

  @AfterClass
  public static void tearDownClass() {
    if (verifyCluster != null) {
      verifyCluster.disconnect();
    }
  }

  private static ConnectionConfiguration config() {
    return ConnectionConfiguration.create(COUCHBASE.getConnectionString())
        .withUsername(COUCHBASE.getUsername())
        .withPassword(COUCHBASE.getPassword())
        .withBucket(BUCKET);
  }

  /** Each test uses its own ID prefix because all tests share one bucket. */
  private static List<String> ids(String prefix, int n) {
    return IntStream.range(0, n).mapToObj(i -> prefix + "-" + i).collect(Collectors.toList());
  }

  private static JsonObject content(String id) {
    return JsonObject.create().put("id", id).put("source", "beam");
  }

  private static CouchbaseIO.Write<String> write(ConnectionConfiguration config) {
    return CouchbaseIO.<String>write()
        .withConnectionConfiguration(config)
        .withIdFn((String id) -> id)
        .withContentFn(CouchbaseIOWriteTest::content);
  }

  private static void assertWritten(Collection collection, List<String> ids) {
    for (String id : ids) {
      assertEquals(content(id), collection.get(id).contentAsObject());
    }
  }

  @Test
  public void testWrite() {
    List<String> ids = ids("basic", 10);
    p.apply(Create.of(ids)).apply(write(config()));
    p.run().waitUntilFinish();

    assertWritten(verifyBucket.defaultCollection(), ids);
  }

  /** Simulates a retried bundle: some documents already exist from a partially applied attempt. */
  @Test
  public void testWriteOverwritesExistingDocuments() {
    List<String> ids = ids("retry", 10);
    Collection collection = verifyBucket.defaultCollection();
    for (String id : ids.subList(0, 5)) {
      collection.upsert(id, JsonObject.create().put("stale", true));
    }

    p.apply(Create.of(ids)).apply(write(config()));
    p.run().waitUntilFinish();

    assertWritten(collection, ids);
  }

  @Test
  public void testWriteToNonDefaultCollection() {
    List<String> ids = ids("collection", 10);
    p.apply(Create.of(ids)).apply(write(config().withScope(SCOPE).withCollection(COLLECTION)));
    p.run().waitUntilFinish();

    assertWritten(verifyBucket.scope(SCOPE).collection(COLLECTION), ids);
  }

  /** 10 documents with batch size 3 exercises both flush-when-full and flush in FinishBundle. */
  @Test
  public void testWriteWithSmallBatchSize() {
    List<String> ids = ids("batch", 10);
    p.apply(Create.of(ids)).apply(write(config()).withBatchSize(3).withMaxConcurrency(2));
    p.run().waitUntilFinish();

    assertWritten(verifyBucket.defaultCollection(), ids);
  }

  @Test
  public void testWrongPasswordFailsWithoutLeakingIt() {
    String wrongPassword = "wrong-s3cr3t-do-not-log";
    p.apply(Create.of(ids("auth", 1))).apply(write(config().withPassword(wrongPassword)));

    // The wrapper type depends on the runner and on which DoFn method failed, so assert on the
    // chain.
    RuntimeException e = assertThrows(RuntimeException.class, () -> p.run().waitUntilFinish());
    String chain =
        Throwables.getCausalChain(e).stream()
            .map(String::valueOf)
            .collect(Collectors.joining("\n"));
    assertThat(chain, containsString("authentication failed"));
    assertThat(chain, not(containsString(wrongPassword)));
  }
}
