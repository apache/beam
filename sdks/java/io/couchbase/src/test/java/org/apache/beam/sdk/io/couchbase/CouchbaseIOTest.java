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

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketInfo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.client.MockClient;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** A couchbase IO test class. */
@RunWith(JUnit4.class)
public class CouchbaseIOTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  protected static MockClient mockClient;
  protected static CouchbaseMock couchbaseMock;
  protected static Cluster cluster;
  protected static com.couchbase.client.java.Bucket bucket;
  protected static int carrierPort;
  protected static int httpPort;
  private static final String BUCKET_NAME = "scientists";
  private static final String USERNAME = "Administration";
  private static final String PWD = "password";

  protected static void getPortInfo(String bucket) throws Exception {
    httpPort = couchbaseMock.getHttpPort();
    carrierPort = couchbaseMock.getCarrierPort(bucket);
  }

  protected static void createMock(@NotNull String bucketName, @NotNull String pwd)
      throws Exception {
    BucketConfiguration bucketConfiguration = new BucketConfiguration();
    bucketConfiguration.numNodes = 1;
    bucketConfiguration.numReplicas = 1;
    bucketConfiguration.numVBuckets = 1024;
    bucketConfiguration.name = bucketName;
    bucketConfiguration.type = Bucket.BucketType.COUCHBASE;
    bucketConfiguration.password = pwd;
    ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
    configList.add(bucketConfiguration);
    couchbaseMock = new CouchbaseMock(0, configList);
    couchbaseMock.start();
    couchbaseMock.waitForStartup();
  }

  protected static void createClient() {
    cluster =
        CouchbaseCluster.create(
            DefaultCouchbaseEnvironment.builder()
                .bootstrapCarrierDirectPort(carrierPort)
                .bootstrapHttpDirectPort(httpPort)
                .build(),
            "couchbase://127.0.0.1");
    bucket = cluster.openBucket(BUCKET_NAME, PWD);
  }

  @Test
  public void testSimple() {
    insertData();
    for (int i = 0; i < 10; i++) {
      JsonDocument doc = bucket.get(i + 1 + "");
      System.out.println(doc);
    }
    BucketInfo info = bucket.bucketManager().info();
    System.out.println(info);
    N1qlQueryResult query =
        bucket.query(N1qlQuery.simple(String.format("SELECT * FROM `%s`", BUCKET_NAME)));
    List<N1qlQueryRow> rows = query.allRows();
    for (N1qlQueryRow row : rows) {
      System.out.println(row.value());
    }
  }

  @BeforeClass
  public static void startCouchbase() throws Exception {
    createMock(BUCKET_NAME, PWD);
    getPortInfo(BUCKET_NAME);
    createClient();
  }

  @AfterClass
  public static void stopCouchbase() {
    if (cluster != null) {
      cluster.disconnect();
    }
    if (couchbaseMock != null) {
      couchbaseMock.stop();
    }
    if (mockClient != null) {
      mockClient.shutdown();
    }
  }

  @Test
  public void testRead() {
    insertData();
    PCollection<JsonDocument> output =
        pipeline.apply(
            CouchbaseIO.read()
                .withHosts(Arrays.asList("couchbase://127.0.0.1"))
                .withHttpPort(httpPort)
                .withCarrierPort(carrierPort)
                .withBucket(BUCKET_NAME)
                .withUsername(USERNAME)
                .withPassword(PWD)
                .withCoder(SerializableCoder.of(JsonDocument.class))
                .withEntity(JsonDocument.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(5L);

    pipeline.run();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void insertData() {
    String[] scientists = {
      "Einstein",
      "Darwin",
      "Copernicus",
      "Pasteur",
      "Curie",
      "Faraday",
      "Newton",
      "Bohr",
      "Galilei",
      "Maxwell"
    };
    for (int i = 0; i < scientists.length; i++) {
      bucket.upsert(
          JsonDocument.create(
              String.valueOf(i + 1), JsonObject.create().put("name", scientists[i])));
    }
  }

  /** Simple Cassandra entity used in test. */
  static class Scientist implements Serializable {

    String name;

    int id;

    @Override
    public String toString() {
      return id + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Scientist scientist = (Scientist) o;
      return id == scientist.id && Objects.equal(name, scientist.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, id);
    }
  }
}
