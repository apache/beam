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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

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

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.client.MockClient;

/** A couchbase IO test class. */
@RunWith(JUnit4.class)
public class CouchbaseIOTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  protected static final BucketConfiguration bucketConfiguration = new BucketConfiguration();
  protected static MockClient mockClient;
  protected static CouchbaseMock couchbaseMock;
  protected static Cluster cluster;
  protected static com.couchbase.client.java.Bucket bucket;
  protected static int carrierPort;
  protected static int httpPort;

  protected static void getPortInfo(String bucket) throws Exception {
    httpPort = couchbaseMock.getHttpPort();
    carrierPort = couchbaseMock.getCarrierPort(bucket);
  }

  protected static void createMock(@NotNull String name, @NotNull String password) throws Exception {
    bucketConfiguration.numNodes = 1;
    bucketConfiguration.numReplicas = 1;
    bucketConfiguration.numVBuckets = 1024;
    bucketConfiguration.name = name;
    bucketConfiguration.type = Bucket.BucketType.COUCHBASE;
    bucketConfiguration.password = password;
    ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
    configList.add(bucketConfiguration);
    couchbaseMock = new CouchbaseMock(0, configList);
    couchbaseMock.start();
    couchbaseMock.waitForStartup();
  }

  protected static void createClient() {
    cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder()
            .bootstrapCarrierDirectPort(carrierPort)
            .bootstrapHttpDirectPort(httpPort)
            .build(), "couchbase://127.0.0.1");
    bucket = cluster.openBucket("default");
  }

  @Test
  public void testSimple() {
    bucket.upsert(JsonDocument.create("foo"));
    bucket.get(JsonDocument.create("foo"));
  }

  @BeforeClass
  public static void startCouchbase() throws Exception {
    createMock("default", "");
    getPortInfo("default");
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
    PCollection<Scientist> output =
        pipeline.apply(
            CouchbaseIO.<Scientist>read()
                .withHosts(Arrays.asList("localhost"))
                .withPort(8091)
                .withBucket("default")
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(1L);

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
      bucket.upsert(JsonDocument.create(String.valueOf(i + 1), JsonObject.create().put("name", scientists[i])));
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
