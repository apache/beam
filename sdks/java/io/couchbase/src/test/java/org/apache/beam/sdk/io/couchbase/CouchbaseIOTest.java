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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import java.io.Serializable;
import java.util.Arrays;
import junit.framework.TestCase;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.couchbase.CouchbaseContainer;

/** Test for {@link CouchbaseIO}. */
@RunWith(JUnit4.class)
public class CouchbaseIOTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static CouchbaseContainer couchbase;
  private static Bucket bucket;
  private static final int CARRIER_PORT = 11210;
  private static final int HTTP_PORT = 8091;
  private static final String BUCKET_NAME = "bucket-name";
  private static final String USERNAME = "admin";
  private static final String ADMIN_PWD = "admin-pwd";
  private static final String BUCKET_PWD = "bucket-pwd";
  private static final int SAMPLE_SIZE = 100;

  @BeforeClass
  public static void startCouchbase() {
    couchbase =
        new CouchbaseContainer()
            .withClusterAdmin(USERNAME, ADMIN_PWD)
            .withNewBucket(
                DefaultBucketSettings.builder()
                    .enableFlush(true)
                    .name(BUCKET_NAME)
                    .quota(100)
                    .password(BUCKET_PWD)
                    .type(BucketType.COUCHBASE)
                    .build());
    couchbase.start();
    bucket = couchbase.getCouchbaseCluster().openBucket(BUCKET_NAME);
    insertData();
  }

  @AfterClass
  public static void stopCouchbase() {
    if (couchbase.isIndex() && couchbase.isQuery() && couchbase.isPrimaryIndex()) {
      bucket.query(
          N1qlQuery.simple(
              String.format("DELETE FROM `%s`", bucket.name()),
              N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)));
    } else {
      bucket.bucketManager().flush();
    }
  }

  @Test
  public void testEstimatedKeySize() throws Exception {
    CouchbaseIO.Read read =
        CouchbaseIO.read()
            .withHosts(Arrays.asList(couchbase.getContainerIpAddress()))
            .withHttpPort(couchbase.getMappedPort(HTTP_PORT))
            .withCarrierPort(couchbase.getMappedPort(CARRIER_PORT))
            .withBucket(BUCKET_NAME)
            .withPassword(BUCKET_PWD);
    CouchbaseIO.CouchbaseSource source = new CouchbaseIO.CouchbaseSource(read);
    long resultSize = source.getEstimatedSizeBytes(PipelineOptionsFactory.create());
    TestCase.assertEquals(SAMPLE_SIZE, resultSize);
  }

  @Test
  public void testRead() {
    PCollection<JsonDocument> output =
        pipeline.apply(
            CouchbaseIO.read()
                .withHosts(Arrays.asList(couchbase.getContainerIpAddress()))
                .withHttpPort(couchbase.getMappedPort(HTTP_PORT))
                .withCarrierPort(couchbase.getMappedPort(CARRIER_PORT))
                .withBucket(BUCKET_NAME)
                .withPassword(BUCKET_PWD));

    PAssert.thatSingleton(output.apply("Count", Count.globally()))
        .isEqualTo(Long.valueOf(SAMPLE_SIZE));

    pipeline.run();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private static void insertData() {
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
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      bucket.upsert(
          JsonDocument.create(
              String.valueOf(i + 1),
              JsonObject.create().put("name", scientists[i % scientists.length])));
    }
  }
}
