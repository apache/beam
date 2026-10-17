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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryScanConsistency;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.couchbase.CouchbaseIO.ConnectionConfiguration;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.RowJson.UnsupportedRowJsonException;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
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

  private static CouchbaseIO.Read<JsonObject> fullRead() {
    return CouchbaseIO.read()
        .withConnectionConfiguration(fullConfig())
        .withQuery("SELECT a.* FROM airline a");
  }

  private IllegalArgumentException applyAndExpectFailure(CouchbaseIO.Read<?> read) {
    return assertThrows(IllegalArgumentException.class, () -> p.apply(read));
  }

  @Test
  public void testReadDefaults() {
    CouchbaseIO.Read<JsonObject> read = CouchbaseIO.read();
    assertEquals(QueryScanConsistency.NOT_BOUNDED, read.scanConsistency());
    assertEquals(JsonObjectCoder.of(), read.coder());
    assertFalse(read.redistribute());
  }

  @Test
  public void testValidReadApplies() {
    PCollection<JsonObject> docs = p.apply(fullRead());
    assertEquals(JsonObjectCoder.of(), docs.getCoder());
  }

  @Test
  public void testReadMissingConnectionConfiguration() {
    assertThat(
        applyAndExpectFailure(CouchbaseIO.read().withQuery("SELECT 1")).getMessage(),
        containsString("withConnectionConfiguration() is required"));
  }

  @Test
  public void testReadMissingQuery() {
    assertThat(
        applyAndExpectFailure(CouchbaseIO.read().withConnectionConfiguration(fullConfig()))
            .getMessage(),
        containsString("withQuery() is required"));
  }

  @Test
  public void testReadMissingPassword() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("couchbase://localhost").withUsername("user");
    assertThat(
        applyAndExpectFailure(fullRead().withConnectionConfiguration(config)).getMessage(),
        containsString("withPassword() is required"));
  }

  @Test
  public void testReadWithoutBucketApplies() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("couchbase://localhost")
            .withUsername("user")
            .withPassword(PASSWORD);
    p.apply(fullRead().withConnectionConfiguration(config));
  }

  @Test
  public void testNamedAndPositionalParametersRejected() {
    CouchbaseIO.Read<JsonObject> read =
        fullRead().withNamedParameter("country", "UK").withPositionalParameters("UK");
    assertThat(applyAndExpectFailure(read).getMessage(), containsString("cannot be combined"));
  }

  @Test
  public void testWithNamedParameterAccumulatesWithoutMutatingEarlierTransform() {
    CouchbaseIO.Read<JsonObject> one = fullRead().withNamedParameter("country", "UK");
    CouchbaseIO.Read<JsonObject> two = one.withNamedParameter("limit", 10);
    assertEquals(JsonObject.create().put("country", "UK"), one.namedParameters());
    assertEquals(JsonObject.create().put("country", "UK").put("limit", 10), two.namedParameters());
  }

  @Test
  public void testWithNamedParameterRejectsUnsupportedType() {
    assertThrows(RuntimeException.class, () -> fullRead().withNamedParameter("when", new Object()));
  }

  @Test
  public void testRedistributeNumKeysRequiresRedistribute() {
    assertThat(
        applyAndExpectFailure(fullRead().withRedistributeNumKeys(4)).getMessage(),
        containsString("requires withRedistribute()"));
  }

  @Test
  public void testNonPositiveRedistributeNumKeysRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> CouchbaseIO.read().withRedistributeNumKeys(0));
  }

  @Test
  public void testWithParseFnLambdaRequiresCoder() {
    CouchbaseIO.Read<String> read = fullRead().withParseFn(row -> row.getString("name"));
    assertThat(applyAndExpectFailure(read).getMessage(), containsString("withCoder()"));
  }

  @Test
  public void testWithParseFnResetsCoder() {
    CouchbaseIO.Read<String> read =
        fullRead().withCoder(JsonObjectCoder.of()).withParseFn(row -> row.getString("name"));
    assertNull(read.coder());
  }

  @Test
  public void testWithParseFnAndCoderApplies() {
    PCollection<String> names =
        p.apply(
            fullRead().withParseFn(row -> row.getString("name")).withCoder(StringUtf8Coder.of()));
    assertEquals(StringUtf8Coder.of(), names.getCoder());
  }

  /** The shuffle must carry the parsed type, so Redistribute has to come after the query DoFn. */
  @Test
  public void testRedistributeAppliedAfterParsing() {
    PCollection<String> names =
        p.apply(
            fullRead()
                .withParseFn(row -> row.getString("name"))
                .withCoder(StringUtf8Coder.of())
                .withRedistribute()
                .withRedistributeNumKeys(4));

    List<String> transforms = new ArrayList<>();
    p.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            transforms.add(node.getFullName());
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            transforms.add(node.getFullName());
          }
        });
    assertThat(
        transforms.indexOf("CouchbaseIO.Read/Redistribute"),
        greaterThan(transforms.indexOf("CouchbaseIO.Read/Query")));
    assertTrue(transforms.contains("CouchbaseIO.Read/Query"));
    assertEquals(StringUtf8Coder.of(), names.getCoder());
  }

  @Test
  public void testReadDisplayDataOmitsPasswordAndParameters() {
    DisplayData displayData =
        DisplayData.from(
            fullRead()
                .withNamedParameter("customer", "secret-customer-id")
                .withScanConsistency(QueryScanConsistency.REQUEST_PLUS)
                .withRedistribute());
    assertThat(displayData, hasDisplayItem("query", "SELECT a.* FROM airline a"));
    assertThat(displayData, hasDisplayItem("scanConsistency", "REQUEST_PLUS"));
    assertThat(displayData, hasDisplayItem("redistribute", true));
    assertThat(displayData, hasDisplayItem("bucket", "beam"));
    assertThat(displayData.toString(), not(containsString(PASSWORD)));
    assertThat(displayData.toString(), not(containsString("secret-customer-id")));
  }

  private static final Schema HUB_SCHEMA = Schema.builder().addStringField("code").build();

  private static final Schema AIRLINE_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addNullableField("rating", Schema.FieldType.DOUBLE)
          .addRowField("hub", HUB_SCHEMA)
          .build();

  private static String causalChain(Throwable t) {
    return Throwables.getCausalChain(t).stream()
        .map(String::valueOf)
        .collect(Collectors.joining("\n"));
  }

  @Test
  public void testReadRowsSetsSchema() {
    PCollection<Row> rows =
        p.apply(
            CouchbaseIO.readRows(AIRLINE_SCHEMA)
                .withConnectionConfiguration(fullConfig())
                .withQuery("SELECT a.* FROM airline a"));
    assertTrue(rows.hasSchema());
    assertEquals(AIRLINE_SCHEMA, rows.getSchema());
  }

  @Test
  public void testReadRowsRejectsUnsupportedSchema() {
    Schema schema = Schema.builder().addByteArrayField("payload").build();
    assertThrows(UnsupportedRowJsonException.class, () -> CouchbaseIO.readRows(schema));
  }

  @Test
  public void testJsonToRowIgnoresExtraFields() {
    JsonObject json =
        JsonObject.create()
            .put("name", "A")
            .put("rating", 4.5)
            .put("hub", JsonObject.create().put("code", "LHR").put("terminal", 5))
            .put("country", "UK");
    Row expected =
        Row.withSchema(AIRLINE_SCHEMA)
            .addValues("A", 4.5, Row.withSchema(HUB_SCHEMA).addValue("LHR").build())
            .build();
    assertEquals(expected, new CouchbaseIO.JsonToRowFn(AIRLINE_SCHEMA).apply(json));
  }

  @Test
  public void testJsonToRowMissingNullableFieldIsNull() {
    JsonObject json =
        JsonObject.create().put("name", "A").put("hub", JsonObject.create().put("code", "LHR"));
    assertNull(new CouchbaseIO.JsonToRowFn(AIRLINE_SCHEMA).apply(json).getValue("rating"));
  }

  @Test
  public void testJsonToRowMissingRequiredFieldFails() {
    JsonObject json = JsonObject.create().put("hub", JsonObject.create().put("code", "LHR"));
    RuntimeException e =
        assertThrows(
            RuntimeException.class, () -> new CouchbaseIO.JsonToRowFn(AIRLINE_SCHEMA).apply(json));
    assertThat(causalChain(e), containsString("'name'"));
  }

  @Test
  public void testJsonToRowTypeMismatchFails() {
    JsonObject json =
        JsonObject.create()
            .put("name", "A")
            .put("rating", "high")
            .put("hub", JsonObject.create().put("code", "LHR"));
    RuntimeException e =
        assertThrows(
            RuntimeException.class, () -> new CouchbaseIO.JsonToRowFn(AIRLINE_SCHEMA).apply(json));
    assertThat(causalChain(e), containsString("'rating'"));
  }

  /** The mapper is transient, so a deserialized function has to build its own. */
  @Test
  public void testJsonToRowWorksAfterSerialization() {
    CouchbaseIO.JsonToRowFn fn =
        SerializableUtils.ensureSerializable(new CouchbaseIO.JsonToRowFn(AIRLINE_SCHEMA));
    JsonObject json =
        JsonObject.create().put("name", "A").put("hub", JsonObject.create().put("code", "LHR"));
    assertEquals("A", fn.apply(json).getString("name"));
  }
}
