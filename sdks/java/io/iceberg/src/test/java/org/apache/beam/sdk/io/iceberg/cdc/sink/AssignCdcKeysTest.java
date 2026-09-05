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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.PortableIcebergDestinations;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AssignCdcKeys}. */
@RunWith(JUnit4.class)
public class AssignCdcKeysTest {

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();
  @Rule public final TestName testName = new TestName();

  private static final String SEQ_COL = CdcWriteConfig.DEFAULT_SEQUENCE_NUMBER_COLUMN;
  private static final int NUM_SHARDS = 8;

  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static final Schema DATA_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("data", Schema.FieldType.STRING)
          .build();

  /** Input schema = data columns + the default sequence-number column. */
  private static final Schema INPUT_SCHEMA =
      Schema.builder().addFields(DATA_SCHEMA.getFields()).addInt64Field(SEQ_COL).build();

  /** {@link #INPUT_SCHEMA} with an additional {@code op} change-type column. */
  private static final Schema INPUT_SCHEMA_WITH_OP =
      Schema.builder().addFields(INPUT_SCHEMA.getFields()).addStringField("op").build();

  /** {@link #INPUT_SCHEMA} but with a nullable sequence-number column. */
  private static final Schema NULLABLE_SEQ_SCHEMA =
      Schema.builder()
          .addFields(DATA_SCHEMA.getFields())
          .addNullableField(SEQ_COL, Schema.FieldType.INT64)
          .build();

  /** {@link #INPUT_SCHEMA} but with a nullable {@code id} (the equality column). */
  private static final Schema NULLABLE_ID_SCHEMA =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT32)
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("data", Schema.FieldType.STRING)
          .addInt64Field(SEQ_COL)
          .build();

  private Catalog catalog;
  private IcebergCatalogConfig catalogConfig;

  @Before
  public void setUp() {
    catalog = CdcSinkTestUtils.hadoopCatalog(tmp.getRoot());
    catalogConfig = CdcSinkTestUtils.catalogConfig(tmp.getRoot());
  }

  private static TableIdentifier uniqueId(String prefix) {
    return TableIdentifier.of("db", prefix + "_" + System.nanoTime());
  }

  private static CdcWriteConfig.Builder cdcWriteConfig() {
    return CdcWriteConfig.builder()
        .setSinkId("test-sink")
        .setNumShards(NUM_SHARDS)
        .setShardsPerPartition(NUM_SHARDS);
  }

  private TableIdentifier createCanonicalTable() {
    String prefix = testName.getMethodName();
    TableIdentifier id = uniqueId(prefix);
    CdcSinkTestUtils.createTable(
        catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
    return id;
  }

  private static Row dataRow(int id, String name, String data, long seq) {
    return Row.withSchema(INPUT_SCHEMA).addValues(id, name, data, seq).build();
  }

  private static Row dataRowWithOp(int id, String name, String data, long seq, String op) {
    return Row.withSchema(INPUT_SCHEMA_WITH_OP).addValues(id, name, data, seq, op).build();
  }

  @SafeVarargs
  private PCollection<Row> input(Schema schema, KV<ValueKind, Row>... rows) {
    return CdcSinkTestUtils.withKinds(p.apply(Create.of(ImmutableList.copyOf(rows))))
        .setRowSchema(schema);
  }

  private PCollectionTuple assignKeys(
      PCollection<Row> in, CdcWriteConfig config, TableIdentifier id) {
    return in.apply(
        new AssignCdcKeys(
            catalogConfig,
            config,
            SingleTableDestinations.of(id, in.getSchema(), config),
            "test-runId"));
  }

  /** Sums the committed values of the named {@link AssignCdcKeys} counter (0 if it never fired). */
  private static long counterTotal(PipelineResult result, String name) {
    Iterable<MetricResult<Long>> counters =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(AssignCdcKeys.class, name))
                    .build())
            .getCounters();
    long total = 0;
    for (MetricResult<Long> counter : counters) {
      total += counter.getCommitted();
    }
    return total;
  }

  /** Asserts the pipeline fails and that some message in the failure's cause chain has tokens. */
  private void assertPipelineFailsMentioning(String... tokens) {
    Pipeline.PipelineExecutionException e =
        assertThrows(Pipeline.PipelineExecutionException.class, () -> p.run().waitUntilFinish());
    StringBuilder messages = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    for (String token : tokens) {
      assertThat(messages.toString(), containsString(token));
    }
  }

  @Test
  public void nativeKindsKeyRecordsWithShardSortKeyAndPayload() {
    TableIdentifier id = createCanonicalTable();
    String dest = id.toString();

    PCollection<Row> rows =
        input(
            INPUT_SCHEMA,
            KV.of(ValueKind.INSERT, dataRow(1, "a", "x", 1L)),
            KV.of(ValueKind.DELETE, dataRow(1, "a", "x", 2L)));

    PCollectionTuple outputs = assignKeys(rows, cdcWriteConfig().build(), id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> list =
                  ImmutableList.copyOf(iter);
              assertThat(list, hasSize(2));
              Set<Integer> shards = new HashSet<>();
              Set<ValueKind> kinds = new HashSet<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : list) {
                assertThat(kv.getKey().getKey(), equalTo(dest));
                int shard = kv.getKey().getValue();
                assertThat(shard, greaterThanOrEqualTo(0));
                assertThat(shard, lessThan(NUM_SHARDS));
                shards.add(shard);
                CdcRecord record = kv.getValue().getValue();
                kinds.add(record.getKind());
                // Sort key is exactly CdcSortKey.encode(pkBytes, seq, kind).
                assertArrayEquals(
                    CdcSortKey.encode(
                        pkBytesForId(record.getData().getInt32("id")),
                        record.getSequenceNumber(),
                        record.getKind()),
                    kv.getValue().getKey());
                // Payload row is projected to the data columns, without the sequence column.
                Row data = record.getData();
                assertThat(data.getSchema().getFieldNames(), contains("id", "name", "data"));
                assertFalse(data.getSchema().hasField(SEQ_COL));
                assertThat(data.getInt32("id"), equalTo(1));
                assertThat(data.getString("name"), equalTo("a"));
                assertThat(data.getString("data"), equalTo("x"));
                assertThat(
                    record.getSequenceNumber(),
                    equalTo(record.getKind() == ValueKind.INSERT ? 1L : 2L));
              }
              // Same primary key => same deterministic shard for both records.
              assertThat(shards, hasSize(1));
              assertThat(kinds, containsInAnyOrder(ValueKind.INSERT, ValueKind.DELETE));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    p.run().waitUntilFinish();
  }

  // -------------------------------------------------------------------------------------------
  // change_type_column paths
  // -------------------------------------------------------------------------------------------

  @Test
  public void changeTypeColumnWithMapResolvesKindsAndIsStripped() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config =
        cdcWriteConfig()
            .setChangeTypeColumn("op")
            .setChangeTypeMap(ImmutableMap.of("c", "INSERT", "u", "UPDATE_AFTER", "d", "DELETE"))
            .build();

    // Native kind is INSERT for all three; the mapped change-type column must override it.
    PCollectionTuple outputs =
        assignKeys(
            input(
                INPUT_SCHEMA_WITH_OP,
                KV.of(ValueKind.INSERT, dataRowWithOp(1, "a", "x", 1L, "c")),
                KV.of(ValueKind.INSERT, dataRowWithOp(2, "b", "y", 2L, "u")),
                KV.of(ValueKind.INSERT, dataRowWithOp(3, "c", "z", 3L, "d"))),
            config,
            id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> list =
                  ImmutableList.copyOf(iter);
              assertThat(list, hasSize(3));
              List<ValueKind> kinds = new ArrayList<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : list) {
                CdcRecord record = kv.getValue().getValue();
                kinds.add(record.getKind());
                // The change-type column is stripped by projection.
                assertFalse(record.getData().getSchema().hasField("op"));
                assertFalse(record.getData().getSchema().hasField(SEQ_COL));
              }
              assertThat(
                  kinds,
                  containsInAnyOrder(ValueKind.INSERT, ValueKind.UPDATE_AFTER, ValueKind.DELETE));
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  public void changeTypeColumnUnmappedValueFallsThroughAsValueKindName() {
    TableIdentifier id = createCanonicalTable();
    // "DELETE" is not a key of the map, so it falls through and parses as a ValueKind name.
    CdcWriteConfig config =
        cdcWriteConfig()
            .setChangeTypeColumn("op")
            .setChangeTypeMap(ImmutableMap.of("c", "INSERT"))
            .build();

    PCollectionTuple outputs =
        assignKeys(
            input(
                INPUT_SCHEMA_WITH_OP,
                KV.of(ValueKind.INSERT, dataRowWithOp(1, "a", "x", 1L, "DELETE"))),
            config,
            id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> list =
                  ImmutableList.copyOf(iter);
              assertThat(list, hasSize(1));
              assertThat(list.get(0).getValue().getValue().getKind(), equalTo(ValueKind.DELETE));
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  public void unknownChangeTypeDivertedToFailedWithErrorHandling() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config =
        cdcWriteConfig().setChangeTypeColumn("op").setErrorHandling(true).build();
    Row poisoned = dataRowWithOp(1, "a", "x", 1L, "bogus");

    PCollectionTuple outputs =
        assignKeys(input(INPUT_SCHEMA_WITH_OP, KV.of(ValueKind.INSERT, poisoned)), config, id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED)).empty();
    PAssert.that(outputs.get(AssignCdcKeys.FAILED))
        .satisfies(
            iter -> {
              List<Row> failed = ImmutableList.copyOf(iter);
              assertThat(failed, hasSize(1));
              String message = failed.get(0).getString("error_message");
              assertThat(message, containsString("bogus"));
              // The message lists the valid ValueKind names ...
              assertThat(message, containsString("INSERT"));
              assertThat(message, containsString("UPDATE_BEFORE"));
              assertThat(failed.get(0).getRow("failed_row"), equalTo(poisoned));
              return null;
            });
    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(counterTotal(result, "failedRecords"), equalTo(1L));
  }

  /**
   * A configured change-type column that is absent from the schema, or null in a row, diverts.
   * {@link WriteCdcRows} rejects such schemas at construction; this pins the stage's own guard.
   */
  @Test
  public void changeTypeColumnAbsentOrNullDiverted() {
    CdcWriteConfig config =
        cdcWriteConfig().setChangeTypeColumn("op").setErrorHandling(true).build();

    // facet: change_type_column configured but the input schema has no such column.
    TableIdentifier absentId = createCanonicalTable();
    PCollectionTuple absent =
        assignKeys(
            input(INPUT_SCHEMA, KV.of(ValueKind.INSERT, dataRow(1, "a", "x", 1L))),
            config,
            absentId);
    PAssert.that(absent.get(AssignCdcKeys.KEYED)).empty();
    PAssert.that(absent.get(AssignCdcKeys.FAILED))
        .satisfies(
            iter -> {
              List<Row> failed = ImmutableList.copyOf(iter);
              assertThat(failed, hasSize(1));
              String message = failed.get(0).getString("error_message");
              assertThat(message, containsString("'op'"));
              assertThat(message, containsString("not found"));
              return null;
            });

    // facet: the column exists but the row's value is null.
    TableIdentifier nullId = createCanonicalTable();
    Schema schema =
        Schema.builder()
            .addFields(INPUT_SCHEMA.getFields())
            .addNullableField("op", Schema.FieldType.STRING)
            .build();
    Row row = Row.withSchema(schema).addValues(1, "a", "x", 1L, null).build();
    PCollectionTuple nullValue =
        CdcSinkTestUtils.withKinds(
                "KindsNullOp",
                p.apply("CreateNullOp", Create.of(ImmutableList.of(KV.of(ValueKind.INSERT, row)))))
            .setRowSchema(schema)
            .apply(
                "AssignNullOp",
                new AssignCdcKeys(
                    catalogConfig,
                    config,
                    SingleTableDestinations.of(nullId, schema, config),
                    "test-runId"));
    PAssert.that(nullValue.get(AssignCdcKeys.KEYED)).empty();
    PAssert.that(nullValue.get(AssignCdcKeys.FAILED))
        .satisfies(
            iter -> {
              List<Row> failed = ImmutableList.copyOf(iter);
              assertThat(failed, hasSize(1));
              String message = failed.get(0).getString("error_message");
              assertThat(message, containsString("'op'"));
              assertThat(message, containsString("null"));
              return null;
            });
    p.run().waitUntilFinish();
  }

  /**
   * A MIXED batch: the good records must still be keyed and only the poison diverted; an all-poison
   * batch cannot tell "divert the poison" from "divert everything".
   */
  @Test
  public void mixedBatchKeepsGoodRecordsAndDivertsOnlyPoison() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config =
        cdcWriteConfig().setChangeTypeColumn("op").setErrorHandling(true).build();

    PCollectionTuple outputs =
        assignKeys(
            input(
                INPUT_SCHEMA_WITH_OP,
                KV.of(ValueKind.INSERT, dataRowWithOp(1, "a", "x", 1L, "INSERT")),
                KV.of(ValueKind.INSERT, dataRowWithOp(2, "b", "y", 2L, "bogus")),
                KV.of(ValueKind.INSERT, dataRowWithOp(3, "c", "z", 3L, "DELETE"))),
            config,
            id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Map<Integer, ValueKind> kindById = new HashMap<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : iter) {
                CdcRecord record = kv.getValue().getValue();
                kindById.put(record.getData().getInt32("id"), record.getKind());
              }
              // The two healthy records survive, with their own resolved kinds.
              assertThat(
                  kindById, equalTo(ImmutableMap.of(1, ValueKind.INSERT, 3, ValueKind.DELETE)));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED))
        .satisfies(
            iter -> {
              List<Row> failed = ImmutableList.copyOf(iter);
              assertThat(failed, hasSize(1));
              assertThat(failed.get(0).getRow("failed_row").getInt32("id"), equalTo(2));
              assertThat(failed.get(0).getString("error_message"), containsString("bogus"));
              return null;
            });
    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(counterTotal(result, "failedRecords"), equalTo(1L));
  }

  @Test
  public void unknownChangeTypeFailsPipelineWithoutErrorHandling() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config = cdcWriteConfig().setChangeTypeColumn("op").build();

    assignKeys(
        input(
            INPUT_SCHEMA_WITH_OP, KV.of(ValueKind.INSERT, dataRowWithOp(1, "a", "x", 1L, "bogus"))),
        config,
        id);

    assertPipelineFailsMentioning("bogus");
  }

  // -------------------------------------------------------------------------------------------
  // Sequence number handling
  // -------------------------------------------------------------------------------------------

  /**
   * A null sequence value diverts. {@link WriteCdcRows} rejects nullable declarations at
   * construction; this pins the stage's own guard.
   */
  @Test
  public void nullSequenceValueDivertedToFailedWithErrorHandling() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config = cdcWriteConfig().setErrorHandling(true).build();
    Row noSeq = Row.withSchema(NULLABLE_SEQ_SCHEMA).addValues(1, "a", "x", null).build();

    PCollectionTuple outputs =
        assignKeys(input(NULLABLE_SEQ_SCHEMA, KV.of(ValueKind.DELETE, noSeq)), config, id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED)).empty();
    PAssert.that(outputs.get(AssignCdcKeys.FAILED))
        .satisfies(
            iter -> {
              List<Row> failed = ImmutableList.copyOf(iter);
              assertThat(failed, hasSize(1));
              assertThat(failed.get(0).getString("error_message"), containsString("sequence"));
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  public void nullSequenceValueFailsPipelineWithoutErrorHandling() {
    TableIdentifier id = createCanonicalTable();
    Row noSeq = Row.withSchema(NULLABLE_SEQ_SCHEMA).addValues(1, "a", "x", null).build();

    // INSERT included: a null sequence value is poison on every kind (no defaulting).
    assignKeys(
        input(NULLABLE_SEQ_SCHEMA, KV.of(ValueKind.INSERT, noSeq)), cdcWriteConfig().build(), id);

    assertPipelineFailsMentioning("sequence");
  }

  // -------------------------------------------------------------------------------------------
  // Upsert mode
  // -------------------------------------------------------------------------------------------

  @Test
  public void upsertDropsUpdateBeforeWithoutFailure() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config = cdcWriteConfig().setUpsert(true).build();

    PCollectionTuple outputs =
        assignKeys(
            input(
                INPUT_SCHEMA,
                KV.of(ValueKind.UPDATE_BEFORE, dataRow(1, "a", "old", 1L)),
                KV.of(ValueKind.UPDATE_AFTER, dataRow(1, "a", "new", 2L))),
            config,
            id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> list =
                  ImmutableList.copyOf(iter);
              assertThat(list, hasSize(1));
              CdcRecord record = list.get(0).getValue().getValue();
              assertThat(record.getKind(), equalTo(ValueKind.UPDATE_AFTER));
              assertThat(record.getData().getString("data"), equalTo("new"));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(counterTotal(result, "upsertUpdateBeforeDropped"), equalTo(1L));
  }

  @Test
  public void upsertDropsUpdateBeforeWithNullSequenceSilently() {
    TableIdentifier id = createCanonicalTable();
    // An upsert feed's before-image may carry no sequence number; it is dropped before the
    // sequence is read, so it must neither be keyed nor diverted as a poison record.
    CdcWriteConfig config = cdcWriteConfig().setUpsert(true).setErrorHandling(true).build();
    Row before = Row.withSchema(NULLABLE_SEQ_SCHEMA).addValues(1, "a", "old", null).build();

    PCollectionTuple outputs =
        assignKeys(input(NULLABLE_SEQ_SCHEMA, KV.of(ValueKind.UPDATE_BEFORE, before)), config, id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED)).empty();
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(counterTotal(result, "upsertUpdateBeforeDropped"), equalTo(1L));
    assertThat(counterTotal(result, "failedRecords"), equalTo(0L));
  }

  // -------------------------------------------------------------------------------------------
  // Sort key: built from the resolved kind
  // -------------------------------------------------------------------------------------------

  /**
   * The sort key's kind byte must come from the resolved kind, not the element's native {@link
   * ValueKind}.
   */
  @Test
  public void sortKeyUsesResolvedKindNotElementNativeKind() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config =
        cdcWriteConfig()
            .setChangeTypeColumn("op")
            .setChangeTypeMap(ImmutableMap.of("b", "UPDATE_BEFORE", "u", "UPDATE_AFTER"))
            .build();

    // Both natively INSERT, both at sequence 5: ONLY the resolved kinds can order them.
    PCollectionTuple outputs =
        assignKeys(
            input(
                INPUT_SCHEMA_WITH_OP,
                KV.of(ValueKind.INSERT, dataRowWithOp(1, "a", "after", 5L, "u")),
                KV.of(ValueKind.INSERT, dataRowWithOp(1, "a", "before", 5L, "b"))),
            config,
            id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Map<ValueKind, byte[]> keyByKind = new HashMap<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : iter) {
                keyByKind.put(kv.getValue().getValue().getKind(), kv.getValue().getKey());
              }
              assertThat(
                  keyByKind.keySet(),
                  containsInAnyOrder(ValueKind.UPDATE_BEFORE, ValueKind.UPDATE_AFTER));
              byte[] before = keyByKind.get(ValueKind.UPDATE_BEFORE);
              byte[] after = keyByKind.get(ValueKind.UPDATE_AFTER);
              assertArrayEquals(
                  CdcSortKey.encode(pkBytesForId(1), 5L, ValueKind.UPDATE_BEFORE), before);
              assertArrayEquals(
                  CdcSortKey.encode(pkBytesForId(1), 5L, ValueKind.UPDATE_AFTER), after);
              // The shared pk and sequence make the kind byte the whole ordering: the before-image
              // must sort strictly first under the byte comparator the shuffle sorter uses.
              assertThat(
                  UnsignedBytes.lexicographicalComparator().compare(before, after), lessThan(0));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    p.run().waitUntilFinish();
  }

  // -------------------------------------------------------------------------------------------
  // Sort key: prefixed by the encoded primary key
  // -------------------------------------------------------------------------------------------

  /**
   * The sort key's leading bytes must be the length-prefixed {@code pkCoder} encoding of the
   * record's primary key: the shuffle sorter makes one key's records contiguous by comparing that
   * prefix, and the writer will read block boundaries off it without decoding rows.
   */
  @Test
  public void sortKeyPrefixIsTheEncodedPrimaryKey() {
    TableIdentifier id = createCanonicalTable();

    PCollectionTuple outputs =
        assignKeys(
            input(
                INPUT_SCHEMA,
                KV.of(ValueKind.INSERT, dataRow(1, "a", "x", 1L)),
                KV.of(ValueKind.DELETE, dataRow(1, "a", "x", 2L)),
                KV.of(ValueKind.INSERT, dataRow(2, "b", "y", 1L))),
            cdcWriteConfig().build(),
            id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              int count = 0;
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : iter) {
                count++;
                byte[] key = kv.getValue().getKey();
                CdcRecord record = kv.getValue().getValue();
                byte[] pkBytes = pkBytesForId(record.getData().getInt32("id"));
                // First 4 bytes carry the pk length big-endian, then the pkCoder bytes follow.
                assertThat(ByteBuffer.wrap(key).getInt(), equalTo(pkBytes.length));
                assertArrayEquals(pkBytes, Arrays.copyOfRange(key, 4, 4 + pkBytes.length));
                assertArrayEquals(
                    CdcSortKey.encode(pkBytes, record.getSequenceNumber(), record.getKind()), key);
              }
              assertThat(count, equalTo(3));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    p.run().waitUntilFinish();
  }

  // -------------------------------------------------------------------------------------------
  // Primary-key extraction
  // -------------------------------------------------------------------------------------------

  /**
   * A nullable-declared equality column is rejected at resolution (table-level, bypassing error
   * handling): the table's identifier columns are required. The per-record null guard still backs
   * this up for rows whose schema drifts after resolution.
   */
  @Test
  public void nullableEqualityColumnSchemaRejectedAtResolution() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config = cdcWriteConfig().setErrorHandling(true).build();
    Row nullId = Row.withSchema(NULLABLE_ID_SCHEMA).addValues(null, "a", "x", 1L).build();

    assignKeys(input(NULLABLE_ID_SCHEMA, KV.of(ValueKind.INSERT, nullId)), config, id);

    assertPipelineFailsMentioning("'id'", "nullable in the input", "required in the table");
  }

  // -------------------------------------------------------------------------------------------
  // shards_per_partition (partition-block sharding)
  // -------------------------------------------------------------------------------------------

  /**
   * Iceberg schema of the partitioned fixture: {@code (id INT, region STRING)} are both required
   * and both equality columns, as partition-block sharding requires of a partition source column.
   */
  private static final org.apache.iceberg.Schema PARTITIONED_ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "region", Types.StringType.get()),
          Types.NestedField.optional(3, "name", Types.StringType.get()));

  private static final Schema PARTITIONED_DATA_SCHEMA =
      IcebergUtils.icebergSchemaToBeamSchema(PARTITIONED_ICEBERG_SCHEMA);

  /** Input schema for {@link #PARTITIONED_ICEBERG_SCHEMA}: its columns plus the sequence column. */
  private static final Schema PARTITIONED_INPUT_SCHEMA =
      Schema.builder()
          .addFields(PARTITIONED_DATA_SCHEMA.getFields())
          .addInt64Field(SEQ_COL)
          .build();

  /**
   * A table partitioned by {@code truncate(region, 2)}: a transform, deliberately, so a shard that
   * followed the raw column value rather than the partition value would be visible.
   */
  private TableIdentifier createTruncatePartitionedTable(String prefix) {
    TableIdentifier id = uniqueId(prefix);
    CdcSinkTestUtils.createTable(
        catalog,
        id,
        PARTITIONED_ICEBERG_SCHEMA,
        ImmutableSet.of(1, 2),
        2,
        PartitionSpec.builderFor(PARTITIONED_ICEBERG_SCHEMA).truncate("region", 2).build());
    return id;
  }

  private static Row partitionedRow(int id, String region, String name, long seq) {
    return Row.withSchema(PARTITIONED_INPUT_SCHEMA).addValues(id, region, name, seq).build();
  }

  /** The {@code pkCoder} bytes of the canonical table's single-{@code id} primary key. */
  private static byte[] pkBytesForId(@Nullable Integer id) {
    Schema pkSchema = Schema.builder().addInt32Field("id").build();
    try {
      return CoderUtils.encodeToByteArray(
          RowCoder.of(pkSchema), Row.withSchema(pkSchema).addValues(id).build());
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  /** The primary-key shard the canonical (unpartitioned) table's single {@code id} column gives. */
  private static int pkShardForId(@Nullable Integer id) {
    return TableSetup.shardFor(pkBytesForId(id), NUM_SHARDS);
  }

  /** The rows used by every block-sharding test: 3 ids in each of 4 regions, over 2 partitions. */
  private PCollection<Row> regionRows() {
    List<KV<ValueKind, Row>> rows = new ArrayList<>();
    int id = 0;
    for (String region : ImmutableList.of("us-east", "us-west", "eu-west", "eu-north")) {
      for (int i = 0; i < 3; i++) {
        rows.add(KV.of(ValueKind.INSERT, partitionedRow(++id, region, "n" + id, 1L)));
      }
    }
    return CdcSinkTestUtils.withKinds(p.apply(Create.of(rows)))
        .setRowSchema(PARTITIONED_INPUT_SCHEMA);
  }

  /**
   * The distinct shards each {@code truncate(region, 2)} partition's records were assigned, keyed
   * by partition value.
   */
  private static Map<String, Set<Integer>> shardsByPartition(
      Iterable<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> keyed) {
    Map<String, Set<Integer>> shards = new HashMap<>();
    for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : keyed) {
      String region = checkStateNotNull(kv.getValue().getValue().getData().getString("region"));
      shards
          .computeIfAbsent(region.substring(0, 2), k -> new HashSet<>())
          .add(kv.getKey().getValue());
    }
    return shards;
  }

  /**
   * At {@code shards_per_partition = 1} every record of a partition gets exactly ONE shard,
   * collapsing a partition's files per commit window down to one.
   */
  @Test
  public void shardsPerPartitionOnePutsEachPartitionOnOneShard() {
    TableIdentifier id = createTruncatePartitionedTable(testName.getMethodName());

    PCollectionTuple outputs =
        assignKeys(regionRows(), cdcWriteConfig().setShardsPerPartition(1).build(), id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Map<String, Set<Integer>> shards = shardsByPartition(iter);
              assertThat(shards.keySet(), containsInAnyOrder("us", "eu"));
              // The transform decides, not the raw column: "us-east" and "us-west" share a shard.
              assertThat(shards.get("us"), hasSize(1));
              assertThat(shards.get("eu"), hasSize(1));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    p.run().waitUntilFinish();
  }

  /**
   * The same input at the uncapped default: primary-key hashing scatters each partition across
   * several shards, the per-partition file multiplication the cap removes.
   */
  @Test
  public void primaryKeyShardingScattersEachPartitionAcrossShards() {
    TableIdentifier id = createTruncatePartitionedTable(testName.getMethodName());

    PCollectionTuple outputs = assignKeys(regionRows(), cdcWriteConfig().build(), id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Map<String, Set<Integer>> shards = shardsByPartition(iter);
              assertThat(shards.get("us").size(), greaterThan(1));
              assertThat(shards.get("eu").size(), greaterThan(1));
              return null;
            });
    p.run().waitUntilFinish();
  }

  /**
   * At EVERY {@code shards_per_partition} setting (1, an interior 4-of-8 with the PK-derived offset
   * live rather than pinned to zero, and the uncapped default) every record carrying one primary
   * key lands on one shard, across all four change kinds and differing non-key columns. This is the
   * invariant the delta writer's same-commit dedup rides on; partition columns are a subset of the
   * equality columns, so it is the real invariant.
   */
  @Test
  public void everyRecordOfOneKeyLandsOnOneShardAtEveryShardsPerPartitionSetting() {
    assertOneKeyOneShard(1);
    assertOneKeyOneShard(4);
    assertOneKeyOneShard(NUM_SHARDS);
    p.run().waitUntilFinish();
  }

  /** Applies stage 1 at the given cap over one key's four kinds and asserts a single shard. */
  private void assertOneKeyOneShard(int shardsPerPartition) {
    TableIdentifier id =
        createTruncatePartitionedTable(testName.getMethodName() + shardsPerPartition);
    PCollection<Row> in =
        CdcSinkTestUtils.withKinds(
                "KindsKeySpp" + shardsPerPartition,
                p.apply(
                    "CreateKeySpp" + shardsPerPartition,
                    Create.of(
                        ImmutableList.of(
                            KV.of(ValueKind.INSERT, partitionedRow(7, "us-east", "a", 1L)),
                            KV.of(ValueKind.UPDATE_BEFORE, partitionedRow(7, "us-east", "a", 2L)),
                            KV.of(ValueKind.UPDATE_AFTER, partitionedRow(7, "us-east", "b", 2L)),
                            KV.of(ValueKind.DELETE, partitionedRow(7, "us-east", "b", 3L))))))
            .setRowSchema(PARTITIONED_INPUT_SCHEMA);

    CdcWriteConfig config = cdcWriteConfig().setShardsPerPartition(shardsPerPartition).build();
    PCollectionTuple outputs =
        in.apply(
            "AssignKeySpp" + shardsPerPartition,
            new AssignCdcKeys(
                catalogConfig,
                config,
                SingleTableDestinations.of(id, PARTITIONED_INPUT_SCHEMA, config),
                "test-runId"));

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> list =
                  ImmutableList.copyOf(iter);
              assertThat(list, hasSize(4));
              Set<Integer> shards = new HashSet<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : list) {
                shards.add(kv.getKey().getValue());
              }
              assertThat(shards, hasSize(1));
              return null;
            });
  }

  /**
   * Determinism, pinned end to end: a change anywhere in the hash chain shows up here instead of
   * silently resharding. A failure is not automatically a bug (an Iceberg {@code JavaHash} change
   * moves these values harmlessly) but must be a conscious update: the same change could mean stage
   * 1 and the writer drifted apart.
   */
  @Test
  public void partitionShardIsPinnedForKnownPartitionValues() {
    TableIdentifier id = createTruncatePartitionedTable(testName.getMethodName());

    // shards_per_partition = 1 must reproduce the pure partition-affine values (offset 0) exactly.
    PCollectionTuple outputs =
        assignKeys(regionRows(), cdcWriteConfig().setShardsPerPartition(1).build(), id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Map<String, Set<Integer>> shards = shardsByPartition(iter);
              assertThat(shards.get("us"), contains(3));
              assertThat(shards.get("eu"), contains(0));
              return null;
            });
    p.run().waitUntilFinish();
  }

  /**
   * An unpartitioned table ignores the cap (with a WARN) and keeps primary-key sharding: rejecting
   * would fail a whole dynamic-destination fleet over one table, honoring it literally would funnel
   * the table through a single shard.
   */
  @Test
  public void unpartitionedTableIgnoresShardsPerPartition() {
    TableIdentifier id = createCanonicalTable();
    CdcWriteConfig config = cdcWriteConfig().setShardsPerPartition(1).build();

    // The gate never builds a plan for an unpartitioned destination, whatever the cap says.
    TableSetup setup =
        new TableSetup(
            catalogConfig, config, DynamicDestinations.singleTable(id, DATA_SCHEMA), "test-runId");
    assertThat(setup.get(id.toString(), DATA_SCHEMA).partitionShardPlan(), nullValue());

    List<KV<ValueKind, Row>> rows = new ArrayList<>();
    for (int i = 1; i <= 32; i++) {
      rows.add(KV.of(ValueKind.INSERT, dataRow(i, "n" + i, "d", 1L)));
    }
    PCollection<Row> in =
        CdcSinkTestUtils.withKinds(p.apply(Create.of(rows))).setRowSchema(INPUT_SCHEMA);

    PCollectionTuple outputs = assignKeys(in, config, id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Set<Integer> shards = new HashSet<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : iter) {
                // Identical to the primary-key shard: the cap was ignored, not applied.
                assertThat(
                    kv.getKey().getValue(),
                    equalTo(pkShardForId(kv.getValue().getValue().getData().getInt32("id"))));
                shards.add(kv.getKey().getValue());
              }
              // The plain primary-key spread, not one funnelled shard.
              assertThat(shards.size(), greaterThan(1));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    p.run().waitUntilFinish();
  }

  /**
   * The block guarantee: a partition occupies exactly {@code spp} CONSECUTIVE shards starting at
   * its {@code spp = 1} shard. Enough distinct keys in one partition cover every offset residue, so
   * the observed shard set must be exactly that block.
   */
  @Test
  public void partitionBlockOccupiesExactlySppConsecutiveShards() {
    TableIdentifier id = createTruncatePartitionedTable(testName.getMethodName());
    int numShards = 16;
    int spp = 4;

    // >= 64 distinct PKs, all in the single truncate(region, 2) partition "us".
    List<KV<ValueKind, Row>> rows = new ArrayList<>();
    for (int i = 1; i <= 64; i++) {
      rows.add(KV.of(ValueKind.INSERT, partitionedRow(i, "us-east", "n" + i, 1L)));
    }
    PCollection<Row> in =
        CdcSinkTestUtils.withKinds(p.apply(Create.of(rows))).setRowSchema(PARTITIONED_INPUT_SCHEMA);

    // base = the shard observed with spp=1, computed through the destination's real plan.
    int base =
        baseShardFor(
            id,
            Row.withSchema(PARTITIONED_DATA_SCHEMA).addValues(1, "us-east", "n1").build(),
            numShards);
    Set<Integer> block = new HashSet<>();
    for (int k = 0; k < spp; k++) {
      block.add(Math.floorMod(base + k, numShards));
    }
    Set<Integer> expectedShards = ImmutableSet.copyOf(block);

    PCollectionTuple outputs =
        assignKeys(
            in, cdcWriteConfig().setNumShards(numShards).setShardsPerPartition(spp).build(), id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Set<Integer> shards = new HashSet<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : iter) {
                shards.add(kv.getKey().getValue());
              }
              // Exactly spp distinct shards, and they are {base..base+spp-1} (mod numShards).
              assertThat(shards, equalTo(expectedShards));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    p.run().waitUntilFinish();
  }

  /**
   * {@code shards_per_partition == num_shards} bypasses the plan entirely: each record's shard is
   * the PLAIN primary-key shard, bit-for-bit, not a block-derived shard that happens to spread.
   */
  @Test
  public void sppEqualToNumShardsBypassesThePlan() {
    TableIdentifier id = createTruncatePartitionedTable(testName.getMethodName());
    int numShards = 16;
    CdcWriteConfig config =
        cdcWriteConfig().setNumShards(numShards).setShardsPerPartition(numShards).build();

    // The gate never builds a plan when spp == num_shards, partitioned or not.
    TableSetup setup =
        new TableSetup(
            catalogConfig,
            config,
            DynamicDestinations.singleTable(id, PARTITIONED_DATA_SCHEMA),
            "test-runId");
    assertThat(setup.get(id.toString(), PARTITIONED_DATA_SCHEMA).partitionShardPlan(), nullValue());

    List<KV<ValueKind, Row>> rows = new ArrayList<>();
    Map<Integer, Integer> byId = new HashMap<>();
    for (int i = 1; i <= 8; i++) {
      rows.add(KV.of(ValueKind.INSERT, partitionedRow(i, "us-east", "n" + i, 1L)));
      byId.put(i, partitionedPkShard(i, "us-east", numShards));
    }
    Map<Integer, Integer> expectedShardById = ImmutableMap.copyOf(byId);
    PCollection<Row> in =
        CdcSinkTestUtils.withKinds(p.apply(Create.of(rows))).setRowSchema(PARTITIONED_INPUT_SCHEMA);

    PCollectionTuple outputs = assignKeys(in, config, id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : iter) {
                int rowId = checkStateNotNull(kv.getValue().getValue().getData().getInt32("id"));
                assertThat(kv.getKey().getValue(), equalTo(expectedShardById.get(rowId)));
              }
              return null;
            });
    p.run().waitUntilFinish();
  }

  /**
   * The composite shard function is part of the sink's cross-version contract: an in-place update
   * mid-window with a changed function splits a primary key across shards and breaks same-commit
   * dedup. This test confirms the breakage.
   */
  @Test
  public void blockShardsAreGoldenPinned() {
    TableIdentifier id = createTruncatePartitionedTable(testName.getMethodName());
    Row fixed = partitionedRow(7, "us-east", "a", 1L);

    // pinned: changing any of these re-shards live pipelines on in-place update.
    // spp=1 is the block base (avalanched partition hash mod 16); spp=4 offsets it by
    // pkHash mod 4 (here +2, inside the block {3,4,5,6}); spp=16 bypasses the plan and is the
    // plain primary-key shard.
    assertBlockShardPinned(id, fixed, 1, 3);
    assertBlockShardPinned(id, fixed, 4, 5);
    assertBlockShardPinned(id, fixed, 16, 2);
    p.run().waitUntilFinish();
  }

  /** Applies stage 1 at the given {@code shards_per_partition} and pins the single row's shard. */
  private void assertBlockShardPinned(TableIdentifier id, Row row, int spp, int expectedShard) {
    PCollection<Row> in =
        CdcSinkTestUtils.withKinds(
                "KindsSpp" + spp,
                p.apply(
                    "CreateSpp" + spp, Create.of(ImmutableList.of(KV.of(ValueKind.INSERT, row)))))
            .setRowSchema(PARTITIONED_INPUT_SCHEMA);
    CdcWriteConfig config = cdcWriteConfig().setNumShards(16).setShardsPerPartition(spp).build();
    PCollectionTuple outputs =
        in.apply(
            "AssignSpp" + spp,
            new AssignCdcKeys(
                catalogConfig,
                config,
                SingleTableDestinations.of(id, PARTITIONED_INPUT_SCHEMA, config),
                "test-runId"));
    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> list =
                  ImmutableList.copyOf(iter);
              assertThat(list, hasSize(1));
              assertThat(list.get(0).getKey().getValue(), equalTo(expectedShard));
              return null;
            });
  }

  /**
   * The {@code shards_per_partition = 1} (base) shard for {@code inputRow}'s partition, computed
   * through the destination's real {@link PartitionShardPlan}.
   */
  private int baseShardFor(TableIdentifier id, Row dataRow, int numShards) {
    TableSetup setup =
        new TableSetup(
            catalogConfig,
            cdcWriteConfig().setNumShards(numShards).setShardsPerPartition(1).build(),
            DynamicDestinations.singleTable(id, PARTITIONED_DATA_SCHEMA),
            "test-runId");
    TableSetup.Dest dest = setup.get(id.toString(), PARTITIONED_DATA_SCHEMA);
    PartitionShardPlan plan = checkStateNotNull(dest.partitionShardPlan());
    return plan.shardFor(dataRow, 0, numShards);
  }

  /** The plain primary-key shard of the partitioned fixture's {@code (id, region)} key. */
  private static int partitionedPkShard(int id, String region, int numShards) {
    Schema pkSchema = Schema.builder().addInt32Field("id").addStringField("region").build();
    try {
      return TableSetup.shardFor(
          CoderUtils.encodeToByteArray(
              RowCoder.of(pkSchema), Row.withSchema(pkSchema).addValues(id, region).build()),
          numShards);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Dynamic (templated) destinations
  // -------------------------------------------------------------------------------------------

  @Test
  public void templateDestinationRoutesToMultipleTables() {
    long suffix = System.nanoTime();
    String tableA = "tmpl_a" + suffix;
    String tableB = "tmpl_b" + suffix;
    // Tables whose columns are (id, dest): the routing column is also a data column.
    CdcSinkTestUtils.createDestTables(catalog, tableA, tableB);

    Schema inputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addNullableField("dest", Schema.FieldType.STRING)
            .addInt64Field(SEQ_COL)
            .build();
    Row rowA = Row.withSchema(inputSchema).addValues(1, tableA, 1L).build();
    Row rowB = Row.withSchema(inputSchema).addValues(2, tableB, 1L).build();

    PCollectionTuple outputs =
        input(inputSchema, KV.of(ValueKind.INSERT, rowA), KV.of(ValueKind.INSERT, rowB))
            .apply(
                new AssignCdcKeys(
                    catalogConfig,
                    cdcWriteConfig().build(),
                    new PortableIcebergDestinations(
                        "db.{dest}",
                        FileFormat.PARQUET.name(),
                        inputSchema,
                        null,
                        null,
                        null,
                        ImmutableList.of(SEQ_COL),
                        null,
                        null),
                    "test-runId"));

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> list =
                  ImmutableList.copyOf(iter);
              assertThat(list, hasSize(2));
              // Bind each record to its destination: row id=1 routed to tableA, id=2 to tableB.
              Map<String, Integer> idByDest = new HashMap<>();
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : list) {
                idByDest.put(
                    kv.getKey().getKey(), kv.getValue().getValue().getData().getInt32("id"));
              }
              assertThat(idByDest, equalTo(ImmutableMap.of("db." + tableA, 1, "db." + tableB, 2)));
              return null;
            });
    PAssert.that(outputs.get(AssignCdcKeys.FAILED)).empty();
    p.run().waitUntilFinish();
  }

  // -------------------------------------------------------------------------------------------
  // Table-level failures bypass error handling
  // -------------------------------------------------------------------------------------------

  @Test
  public void tableConfigExceptionPropagatesDespiteErrorHandling() {
    TableIdentifier id = uniqueId("v1_table");
    CdcSinkTestUtils.createTable(
        catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 1, PartitionSpec.unpartitioned());
    CdcWriteConfig config = cdcWriteConfig().setErrorHandling(true).build();

    assignKeys(input(INPUT_SCHEMA, KV.of(ValueKind.INSERT, dataRow(1, "a", "x", 1L))), config, id);

    assertPipelineFailsMentioning("format-version", "append sink");
  }

  // -------------------------------------------------------------------------------------------
  // Shard distribution
  // -------------------------------------------------------------------------------------------

  @Test
  public void samePkAlwaysSameShardAndDistinctPksSpread() {
    TableIdentifier id = createCanonicalTable();

    // 100 distinct primary keys, two records each (INSERT then DELETE).
    List<KV<ValueKind, Row>> rows = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      rows.add(KV.of(ValueKind.INSERT, dataRow(i, "n" + i, "d", 1L)));
      rows.add(KV.of(ValueKind.DELETE, dataRow(i, "n" + i, "d", 2L)));
    }
    PCollection<Row> in =
        CdcSinkTestUtils.withKinds(p.apply(Create.of(rows))).setRowSchema(INPUT_SCHEMA);

    PCollectionTuple outputs = assignKeys(in, cdcWriteConfig().build(), id);

    PAssert.that(outputs.get(AssignCdcKeys.KEYED))
        .satisfies(
            iter -> {
              Map<Integer, Set<Integer>> shardsByPk = new HashMap<>();
              Set<Integer> allShards = new HashSet<>();
              int count = 0;
              for (KV<KV<String, Integer>, KV<byte[], CdcRecord>> kv : iter) {
                count++;
                int pk = kv.getValue().getValue().getData().getInt32("id");
                int shard = kv.getKey().getValue();
                shardsByPk.computeIfAbsent(pk, unused -> new HashSet<>()).add(shard);
                allShards.add(shard);
              }
              assertThat(count, equalTo(200));
              // Every primary key maps to exactly one shard across its records.
              for (Map.Entry<Integer, Set<Integer>> entry : shardsByPk.entrySet()) {
                assertThat(
                    "pk " + entry.getKey() + " mapped to multiple shards",
                    entry.getValue(),
                    hasSize(1));
              }
              // Distinct primary keys spread over more than one shard.
              assertThat(allShards.size(), greaterThan(1));
              return null;
            });
    p.run().waitUntilFinish();
  }
}
