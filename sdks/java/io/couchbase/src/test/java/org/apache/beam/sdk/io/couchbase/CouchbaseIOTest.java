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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.couchbase.client.java.json.JsonObject;
import org.apache.beam.sdk.io.couchbase.CouchbaseIO.ConnectionConfiguration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CouchbaseIO} configuration and validation. No Couchbase server needed. */
@RunWith(JUnit4.class)
public class CouchbaseIOTest {

  private static final String PASSWORD = "s3cr3t-do-not-log";

  // These tests only apply transforms to check validation; none of the pipelines are run.
  @Rule
  public final transient TestPipeline p =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private static ConnectionConfiguration fullConfig() {
    return ConnectionConfiguration.create("couchbase://localhost")
        .withUsername("user")
        .withPassword(PASSWORD)
        .withBucket("beam");
  }

  private static CouchbaseIO.Write<String> fullWrite() {
    return CouchbaseIO.<String>write()
        .withConnectionConfiguration(fullConfig())
        .withIdFn((String s) -> s)
        .withContentFn((String s) -> JsonObject.create().put("v", s));
  }

  private IllegalArgumentException applyAndExpectFailure(CouchbaseIO.Write<String> write) {
    return assertThrows(IllegalArgumentException.class, () -> p.apply(Create.of("a")).apply(write));
  }

  @Test
  public void testConnectionConfigurationDefaults() {
    ConnectionConfiguration config = ConnectionConfiguration.create("couchbase://localhost");
    assertEquals("_default", config.scope());
    assertEquals("_default", config.collection());
  }

  @Test
  public void testWriteDefaults() {
    CouchbaseIO.Write<String> write = CouchbaseIO.write();
    assertEquals(CouchbaseIO.DEFAULT_BATCH_SIZE, write.batchSize());
    assertEquals(CouchbaseIO.DEFAULT_MAX_CONCURRENCY, write.maxConcurrency());
  }

  @Test
  public void testToStringOmitsPassword() {
    String s = fullConfig().toString();
    assertThat(s, containsString("beam"));
    assertThat(s, not(containsString(PASSWORD)));
  }

  @Test
  public void testDisplayDataOmitsPassword() {
    DisplayData displayData = DisplayData.from(fullWrite().withBatchSize(100));
    assertThat(displayData, hasDisplayItem("connectionString", "couchbase://localhost"));
    assertThat(displayData, hasDisplayItem("username", "user"));
    assertThat(displayData, hasDisplayItem("bucket", "beam"));
    assertThat(displayData, hasDisplayItem("scope", "_default"));
    assertThat(displayData, hasDisplayItem("collection", "_default"));
    assertThat(displayData, hasDisplayItem("batchSize", 100));
    assertThat(displayData, not(hasDisplayItem("password")));
    assertThat(displayData.toString(), not(containsString(PASSWORD)));
  }

  @Test
  public void testValidWriteApplies() {
    p.apply(Create.of("a")).apply(fullWrite());
  }

  @Test
  public void testMissingConnectionConfiguration() {
    CouchbaseIO.Write<String> write =
        CouchbaseIO.<String>write()
            .withIdFn((String s) -> s)
            .withContentFn((String s) -> JsonObject.create());
    assertThat(
        applyAndExpectFailure(write).getMessage(),
        containsString("withConnectionConfiguration() is required"));
  }

  @Test
  public void testMissingIdFn() {
    CouchbaseIO.Write<String> write =
        CouchbaseIO.<String>write()
            .withConnectionConfiguration(fullConfig())
            .withContentFn((String s) -> JsonObject.create());
    assertThat(applyAndExpectFailure(write).getMessage(), containsString("withIdFn() is required"));
  }

  @Test
  public void testMissingContentFn() {
    CouchbaseIO.Write<String> write =
        CouchbaseIO.<String>write()
            .withConnectionConfiguration(fullConfig())
            .withIdFn((String s) -> s);
    assertThat(
        applyAndExpectFailure(write).getMessage(), containsString("withContentFn() is required"));
  }

  @Test
  public void testMissingBucket() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("couchbase://localhost")
            .withUsername("user")
            .withPassword(PASSWORD);
    assertThat(
        applyAndExpectFailure(fullWrite().withConnectionConfiguration(config)).getMessage(),
        containsString("withBucket() is required"));
  }

  @Test
  public void testMissingUsername() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("couchbase://localhost")
            .withPassword(PASSWORD)
            .withBucket("beam");
    assertThat(
        applyAndExpectFailure(fullWrite().withConnectionConfiguration(config)).getMessage(),
        containsString("withUsername() is required"));
  }

  @Test
  public void testMissingPassword() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("couchbase://localhost")
            .withUsername("user")
            .withBucket("beam");
    assertThat(
        applyAndExpectFailure(fullWrite().withConnectionConfiguration(config)).getMessage(),
        containsString("withPassword() is required"));
  }

  @Test
  public void testNonPositiveBatchSizeRejected() {
    assertThrows(IllegalArgumentException.class, () -> CouchbaseIO.write().withBatchSize(0));
  }

  @Test
  public void testNonPositiveMaxConcurrencyRejected() {
    assertThrows(IllegalArgumentException.class, () -> CouchbaseIO.write().withMaxConcurrency(0));
  }
}
