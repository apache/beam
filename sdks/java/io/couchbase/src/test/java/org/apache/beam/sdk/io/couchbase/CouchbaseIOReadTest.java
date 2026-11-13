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
import static org.junit.Assert.assertThrows;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryScanConsistency;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.couchbase.CouchbaseIO.ConnectionConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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

/** Reads from a single-node Couchbase Server started by Testcontainers. */
@RunWith(JUnit4.class)
public class CouchbaseIOReadTest {

  private static final String BUCKET = "beam";
  private static final String SCOPE = "app";
  private static final String COLLECTION = "airlines";

  @ClassRule
  public static final CouchbaseContainer COUCHBASE =
      new CouchbaseContainer("couchbase/server:enterprise-8.0.3")
          // The container creates its index straight after the bucket, which can fail while the
          // new cluster is still rebalancing; setUpClass creates the indexes once the bucket is
          // ready instead.
          .withBucket(new BucketDefinition(BUCKET).withPrimaryIndex(false));

  private static Cluster seedCluster;
  private static Bucket seedBucket;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  // Only for the round-trip test, which runs a write pipeline before the read pipeline.
  @Rule public final transient TestPipeline writePipeline = TestPipeline.create();

  @BeforeClass
  public static void setUpClass() {
    seedCluster =
        Cluster.connect(
            COUCHBASE.getConnectionString(), COUCHBASE.getUsername(), COUCHBASE.getPassword());
    seedBucket = seedCluster.bucket(BUCKET);
    seedBucket.waitUntilReady(Duration.ofSeconds(60));
    seedBucket.collections().createScope(SCOPE);
    seedBucket.collections().createCollection(SCOPE, COLLECTION);
    seedCluster.query(String.format("CREATE PRIMARY INDEX ON `%s`.`_default`.`_default`", BUCKET));
    seedCluster.query(
        String.format("CREATE PRIMARY INDEX ON `%s`.`%s`.`%s`", BUCKET, SCOPE, COLLECTION));
  }

  @AfterClass
  public static void tearDownClass() {
    if (seedCluster != null) {
      seedCluster.disconnect();
    }
  }

  private static ConnectionConfiguration config() {
    return ConnectionConfiguration.create(COUCHBASE.getConnectionString())
        .withUsername(COUCHBASE.getUsername())
        .withPassword(COUCHBASE.getPassword())
        .withBucket(BUCKET);
  }

  /** All tests share one bucket, so each tags its documents and queries only its own tag. */
  private static JsonObject doc(String test, int n) {
    return JsonObject.create().put("test", test).put("n", n);
  }

  private static List<JsonObject> seed(Collection collection, String test, int count) {
    List<JsonObject> docs =
        IntStream.range(0, count).mapToObj(n -> doc(test, n)).collect(Collectors.toList());
    for (JsonObject d : docs) {
      collection.upsert(test + "-" + d.getInt("n"), d);
    }
    return docs;
  }

  private static CouchbaseIO.Read<JsonObject> readTagged(String collection) {
    return CouchbaseIO.read()
        .withConnectionConfiguration(config())
        .withQuery("SELECT d.* FROM `" + collection + "` d WHERE d.test = $test")
        .withScanConsistency(QueryScanConsistency.REQUEST_PLUS);
  }

  @Test
  public void testReadAsJsonObject() {
    List<JsonObject> docs = seed(seedBucket.defaultCollection(), "json", 10);

    PCollection<JsonObject> read =
        p.apply(readTagged("_default").withNamedParameter("test", "json"));

    PAssert.that(read).containsInAnyOrder(docs);
    p.run().waitUntilFinish();
  }

  @Test
  public void testReadEmptyResult() {
    PCollection<JsonObject> read =
        p.apply(readTagged("_default").withNamedParameter("test", "no-such-test"));

    PAssert.that(read).empty();
    p.run().waitUntilFinish();
  }

  @Test
  public void testReadWithParseFnAndPositionalParameters() {
    seed(seedBucket.scope(SCOPE).collection(COLLECTION), "parse", 5);

    PCollection<String> ids =
        p.apply(
            CouchbaseIO.read()
                .withConnectionConfiguration(config().withScope(SCOPE))
                .withQuery("SELECT META(a).id AS id FROM " + COLLECTION + " a WHERE a.test = $1")
                .withPositionalParameters("parse")
                .withScanConsistency(QueryScanConsistency.REQUEST_PLUS)
                .withParseFn(row -> row.getString("id"))
                .withCoder(StringUtf8Coder.of()));

    PAssert.that(ids).containsInAnyOrder("parse-0", "parse-1", "parse-2", "parse-3", "parse-4");
    p.run().waitUntilFinish();
  }

  @Test
  public void testReadAtClusterLevelWithoutBucket() {
    List<JsonObject> docs = seed(seedBucket.scope(SCOPE).collection(COLLECTION), "cluster", 5);
    ConnectionConfiguration noBucket =
        ConnectionConfiguration.create(COUCHBASE.getConnectionString())
            .withUsername(COUCHBASE.getUsername())
            .withPassword(COUCHBASE.getPassword());

    PCollection<JsonObject> read =
        p.apply(
            CouchbaseIO.read()
                .withConnectionConfiguration(noBucket)
                .withQuery(
                    String.format(
                        "SELECT d.* FROM `%s`.`%s`.`%s` d WHERE d.test = $test",
                        BUCKET, SCOPE, COLLECTION))
                .withNamedParameter("test", "cluster")
                .withScanConsistency(QueryScanConsistency.REQUEST_PLUS));

    PAssert.that(read).containsInAnyOrder(docs);
    p.run().waitUntilFinish();
  }

  @Test
  public void testReadWithRedistribute() {
    seed(seedBucket.defaultCollection(), "redistribute", 20);

    PCollection<Long> count =
        p.apply(
                readTagged("_default")
                    .withNamedParameter("test", "redistribute")
                    .withParseFn(row -> row.getInt("n").toString())
                    .withCoder(StringUtf8Coder.of())
                    .withRedistribute()
                    .withRedistributeNumKeys(4))
            .apply(Count.globally());

    PAssert.thatSingleton(count).isEqualTo(20L);
    p.run().waitUntilFinish();
  }

  @Test
  public void testWriteThenReadRoundTrip() {
    List<JsonObject> docs =
        IntStream.range(0, 10).mapToObj(n -> doc("roundtrip", n)).collect(Collectors.toList());
    writePipeline
        .apply(Create.of(docs).withCoder(JsonObjectCoder.of()))
        .apply(
            CouchbaseIO.<JsonObject>write()
                .withConnectionConfiguration(config())
                .withIdFn(d -> "roundtrip-" + d.getInt("n"))
                .withContentFn(d -> d));
    writePipeline.run().waitUntilFinish();

    PCollection<JsonObject> read =
        p.apply(readTagged("_default").withNamedParameter("test", "roundtrip"));

    PAssert.that(read).containsInAnyOrder(docs);
    p.run().waitUntilFinish();
  }

  @Test
  public void testParseFnFailureFailsPipeline() {
    seed(seedBucket.defaultCollection(), "parse-failure", 3);
    p.apply(
        readTagged("_default")
            .withNamedParameter("test", "parse-failure")
            .<String>withParseFn(
                row -> {
                  throw new IllegalStateException("cannot parse row " + row.getInt("n"));
                })
            .withCoder(StringUtf8Coder.of()));

    RuntimeException e = assertThrows(RuntimeException.class, () -> p.run().waitUntilFinish());
    String chain =
        Throwables.getCausalChain(e).stream()
            .map(String::valueOf)
            .collect(Collectors.joining("\n"));
    assertThat(chain, containsString("cannot parse row"));
  }

  /** The documents also carry {@code test}, which the schema doesn't name. */
  @Test
  public void testReadRows() {
    seed(seedBucket.defaultCollection(), "rows", 3);
    Schema schema =
        Schema.builder()
            .addInt32Field("n")
            .addNullableField("missing", Schema.FieldType.STRING)
            .build();

    PCollection<Row> rows =
        p.apply(
            CouchbaseIO.readRows(schema)
                .withConnectionConfiguration(config())
                .withQuery("SELECT d.* FROM `_default` d WHERE d.test = $test")
                .withNamedParameter("test", "rows")
                .withScanConsistency(QueryScanConsistency.REQUEST_PLUS));

    PAssert.that(rows)
        .containsInAnyOrder(
            IntStream.range(0, 3)
                .mapToObj(n -> Row.withSchema(schema).addValues(n, null).build())
                .collect(Collectors.toList()));
    p.run().waitUntilFinish();
  }

  @Test
  public void testReadRowsTypeMismatchFailsPipeline() {
    seed(seedBucket.defaultCollection(), "rows-mismatch", 1);
    Schema schema = Schema.builder().addBooleanField("n").build();
    p.apply(
        CouchbaseIO.readRows(schema)
            .withConnectionConfiguration(config())
            .withQuery("SELECT d.* FROM `_default` d WHERE d.test = $test")
            .withNamedParameter("test", "rows-mismatch")
            .withScanConsistency(QueryScanConsistency.REQUEST_PLUS));

    RuntimeException e = assertThrows(RuntimeException.class, () -> p.run().waitUntilFinish());
    String chain =
        Throwables.getCausalChain(e).stream()
            .map(String::valueOf)
            .collect(Collectors.joining("\n"));
    assertThat(chain, containsString("'n'"));
  }
}
