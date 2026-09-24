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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.DEAD_LETTER_TAG;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.INPUT_TAG;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.SNAPSHOTS_TAG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the {@code cdc} mode of {@link IcebergWriteSchemaTransformProvider}. End-to-end tests
 * drive a real {@link HadoopCatalog} V2 table; each test uses a unique table identifier so the
 * process-wide TableCache never sees a repeat.
 */
@RunWith(JUnit4.class)
public class IcebergWriteSchemaTransformProviderCdcTest {

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  /** Canonical test table schema (id INT primary key, name/data STRING). */
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

  /** The default sequence-number column name (see {@code cdc.sink.CdcWriteConfig}). */
  private static final String DEFAULT_SEQ_COL = "_commit_snapshot_sequence_number";

  /** Input schema = data schema + a {@code change_type} column (string op code). */
  private static final Schema INPUT_SCHEMA_WITH_CHANGE_TYPE =
      Schema.builder().addFields(DATA_SCHEMA.getFields()).addStringField("change_type").build();

  /** {@link #INPUT_SCHEMA_WITH_CHANGE_TYPE} + the default sequence-number column. */
  private static final Schema INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ =
      Schema.builder()
          .addFields(INPUT_SCHEMA_WITH_CHANGE_TYPE.getFields())
          .addInt64Field(DEFAULT_SEQ_COL)
          .build();

  private Catalog catalog;
  private String warehousePath;

  @Before
  public void setUp() {
    warehousePath = tmp.getRoot().getAbsolutePath();
    catalog = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehousePath);
  }

  private Map<String, String> catalogProperties() {
    return ImmutableMap.of(
        "type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, "warehouse", "file:" + warehousePath);
  }

  /** Creates a fresh unpartitioned V2 table (PK = {@code id}) with a unique identifier. */
  private TableIdentifier v2Table() {
    TableIdentifier id = TableIdentifier.of("db", "t" + System.nanoTime());
    createV2Table(id);
    return id;
  }

  private void createV2Table(TableIdentifier id) {
    org.apache.iceberg.Schema schemaWithIds =
        new org.apache.iceberg.Schema(ICEBERG_SCHEMA.columns(), ImmutableSet.of(1));
    catalog.createTable(
        id, schemaWithIds, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "2"));
  }

  /** A config builder pre-wired to this test's catalog and a fresh single table. */
  private Configuration.Builder configFor(TableIdentifier id) {
    return Configuration.builder()
        .setTable(id.toString())
        .setCatalogProperties(catalogProperties());
  }

  /** {@link #configFor} in merge-on-read mode, reading kinds from a {@code change_type} column. */
  private Configuration.Builder cdcConfigFor(TableIdentifier id) {
    return configFor(id).setMode("merge-on-read").setChangeTypeColumn("change_type");
  }

  /** A merge-on-read config builder for the {@code db.{name}} destination template. */
  private Configuration.Builder templateConfig() {
    return Configuration.builder()
        .setTable("db.{name}")
        .setCatalogProperties(catalogProperties())
        .setMode("merge-on-read")
        .setChangeTypeColumn("change_type");
  }

  /**
   * Two fresh V2 tables, {@code db.a_<suffix>} and {@code db.b_<suffix>}, for the template path.
   */
  private List<Table> templateTables(String suffix) {
    TableIdentifier idA = TableIdentifier.of("db", "a_" + suffix);
    TableIdentifier idB = TableIdentifier.of("db", "b_" + suffix);
    createV2Table(idA);
    createV2Table(idB);
    return ImmutableList.of(catalog.loadTable(idA), catalog.loadTable(idB));
  }

  /** The provider's {@link SchemaTransform} for {@code config}. */
  private static SchemaTransform transformFor(Configuration config) {
    return new IcebergWriteSchemaTransformProvider().from(config);
  }

  /** Applies {@code config}'s CDC write to {@code rows}, returning the output tuple. */
  private PCollectionRowTuple applyCdcWrite(Configuration config, Schema schema, Row... rows) {
    PCollection<Row> input = p.apply(Create.of(ImmutableList.copyOf(rows)).withRowSchema(schema));
    return PCollectionRowTuple.of(INPUT_TAG, input).apply("CdcWrite", transformFor(config));
  }

  /** {@link #applyCdcWrite} followed by a pipeline run to completion. */
  private void runCdcWrite(Configuration config, Schema schema, Row... rows) {
    applyCdcWrite(config, schema, rows);
    p.run().waitUntilFinish();
  }

  /** Reads all live rows of {@code table} as sorted {@code id:name:data} strings. */
  private static List<String> readRows(Table table) {
    table.refresh();
    return ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
        .map(r -> r.getField("id") + ":" + r.getField("name") + ":" + r.getField("data"))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  /** An input row with a {@code change_type} op column and an explicit sequence number. */
  private static Row rowWithSeq(int id, String name, String data, String changeType, long seq) {
    return Row.withSchema(INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ)
        .addValues(id, name, data, changeType, seq)
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // Mode gating
  // ---------------------------------------------------------------------------------------------

  /** Options of the other mode are rejected together; shared options pass in both modes. */
  @Test
  public void validateModeOptionsRejectsOptionsOfTheOtherMode() {
    TableIdentifier id = TableIdentifier.of("db", "t");

    Configuration append = configFor(id).setUpsert(true).setNumShards(4).setSinkId("s").build();
    IllegalArgumentException appendError =
        assertThrows(IllegalArgumentException.class, append::validateModeOptions);
    assertThat(
        appendError.getMessage(),
        equalTo(
            "The following options are not supported in 'append' mode yet: "
                + "[upsert, num_shards, sink_id]"));

    Configuration mergeOnRead =
        cdcConfigFor(id).setDistributionMode("hash").setAutosharding(true).build();
    IllegalArgumentException mergeOnReadError =
        assertThrows(IllegalArgumentException.class, mergeOnRead::validateModeOptions);
    assertThat(
        mergeOnReadError.getMessage(),
        equalTo(
            "The following options are not supported in 'merge-on-read' mode yet: "
                + "[distribution_mode, autosharding]"));

    configFor(id).setTriggeringFrequencySeconds(30).build().validateModeOptions();
    cdcConfigFor(id).setTriggeringFrequencySeconds(30).build().validateModeOptions();

    IllegalArgumentException unknown =
        assertThrows(
            IllegalArgumentException.class,
            () -> configFor(id).setMode("copy-on-write").build().validateModeOptions());
    assertThat(unknown.getMessage(), containsString("Unknown mode 'copy-on-write'"));
  }

  // ---------------------------------------------------------------------------------------------
  // Config values with an observable effect
  // ---------------------------------------------------------------------------------------------

  /**
   * {@code equality_columns} is the most dangerous field to lose (a drop falls back to the table's
   * identifier fields and deletes key on the WRONG column): the identifier field is {@code id},
   * {@code equality_columns} names {@code code}, and the DELETE carries a non-matching {@code id} ,
   * only the configured key can apply it.
   */
  @Test
  public void equalityColumnsConfigDecidesTheDeleteKey() {
    TableIdentifier id = TableIdentifier.of("db", "eqcols" + System.nanoTime());
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "code", Types.StringType.get()),
                Types.NestedField.optional(3, "val", Types.StringType.get())),
            ImmutableSet.of(1)); // identifier field = id, deliberately NOT the equality column
    catalog.createTable(
        id, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "2"));
    Table table = catalog.loadTable(id);

    Schema inputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("code")
            .addNullableField("val", Schema.FieldType.STRING)
            .addStringField("change_type")
            .addInt64Field(DEFAULT_SEQ_COL)
            .build();

    runCdcWrite(
        cdcConfigFor(id).setEqualityColumns(ImmutableList.of("code")).build(),
        inputSchema,
        Row.withSchema(inputSchema).addValues(1, "k1", "x", "INSERT", 1L).build(),
        Row.withSchema(inputSchema).addValues(2, "k2", "y", "INSERT", 1L).build(),
        // A DELETE whose id (99) matches nothing: only the `code` key can apply it.
        Row.withSchema(inputSchema).addValues(99, "k1", "x", "DELETE", 2L).build());

    table.refresh();
    assertThat(
        ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
            .map(r -> r.getField("id") + ":" + r.getField("code"))
            .collect(ImmutableList.toImmutableList()),
        containsInAnyOrder("2:k2"));
  }

  // ---------------------------------------------------------------------------------------------
  // Projection
  // ---------------------------------------------------------------------------------------------

  /**
   * The control columns are dropped from the written row by default; listing them in {@code keep}
   * writes their raw values too, for tables that carry a last-change or last-sequence column.
   */
  @Test
  public void keepCanWriteTheControlColumns() {
    TableIdentifier id = TableIdentifier.of("db", "ctl" + System.nanoTime());
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "data", Types.StringType.get()),
                Types.NestedField.required(4, "change_type", Types.StringType.get()),
                Types.NestedField.required(5, DEFAULT_SEQ_COL, Types.LongType.get())),
            ImmutableSet.of(1));
    catalog.createTable(
        id, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "2"));
    Table table = catalog.loadTable(id);

    runCdcWrite(
        cdcConfigFor(id)
            .setKeep(ImmutableList.of("id", "name", "data", "change_type", DEFAULT_SEQ_COL))
            .build(),
        INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ,
        rowWithSeq(1, "a", "x", "INSERT", 1L),
        rowWithSeq(1, "a", "z", "UPDATE_AFTER", 2L),
        rowWithSeq(2, "b", "y", "INSERT", 1L),
        rowWithSeq(2, "b", "y", "DELETE", 2L));

    // The sink still read both columns: id=1 shows its seq=2 image and id=2 was deleted.
    table.refresh();
    assertThat(
        ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
            .map(
                r ->
                    r.getField("id")
                        + ":"
                        + r.getField("data")
                        + ":"
                        + r.getField("change_type")
                        + ":"
                        + r.getField(DEFAULT_SEQ_COL))
            .collect(ImmutableList.toImmutableList()),
        equalTo(ImmutableList.of("1:z:UPDATE_AFTER:2")));
  }

  /**
   * The unnested dead-letter shape re-feeds into a single-table sink: {@code destination} dropped
   * by the projection, the control columns consumed (then stripped) by the sink.
   */
  @Test
  public void singleTableReplaysDeadLetterShape() {
    TableIdentifier id = v2Table();
    Table table = catalog.loadTable(id);

    Schema replaySchema =
        Schema.builder()
            .addFields(DATA_SCHEMA.getFields())
            .addStringField("change_type")
            .addInt64Field("sequence_number")
            .addStringField("destination")
            .build();
    runCdcWrite(
        cdcConfigFor(id)
            .setSequenceNumberColumn("sequence_number")
            .setDrop(ImmutableList.of("destination"))
            .build(),
        replaySchema,
        Row.withSchema(replaySchema).addValues(1, "a", "x", "INSERT", 1L, id.toString()).build());

    assertThat(readRows(table), equalTo(ImmutableList.of("1:a:x")));
  }

  /**
   * A {@code keep} whitelist that omits the control columns must still let both through to the
   * sink: the UPDATE lands and the DELETE deletes only if kinds and order arrived.
   */
  @Test
  public void controlColumnsSurviveKeepProjection() {
    TableIdentifier id = v2Table();
    Table table = catalog.loadTable(id);

    Schema inputSchema =
        Schema.builder()
            .addFields(INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ.getFields())
            .addStringField("source")
            .build();

    // keep lists only the data columns: no control columns, no "source" envelope column.
    runCdcWrite(
        cdcConfigFor(id).setKeep(ImmutableList.of("id", "name", "data")).build(),
        inputSchema,
        Row.withSchema(inputSchema).addValues(1, "a", "x", "INSERT", 1L, "m").build(),
        Row.withSchema(inputSchema).addValues(2, "b", "y", "INSERT", 1L, "m").build(),
        Row.withSchema(inputSchema).addValues(1, "a", "x", "UPDATE_BEFORE", 2L, "m").build(),
        Row.withSchema(inputSchema).addValues(1, "a", "z", "UPDATE_AFTER", 2L, "m").build(),
        Row.withSchema(inputSchema).addValues(2, "b", "y", "DELETE", 2L, "m").build());

    assertThat(readRows(table), equalTo(ImmutableList.of("1:a:z")));
  }

  /**
   * Merge-on-read mode with nothing else set uses every default: kinds come from each element's
   * native {@link ValueKind}, which the projection must preserve (a plain {@code output()} re-emit
   * would stamp everything INSERT).
   */
  @Test
  public void mergeOnReadDefaultsUseNativeValueKinds() {
    TableIdentifier id = v2Table();
    Table table = catalog.loadTable(id);

    Schema inputSchema =
        Schema.builder()
            .addFields(DATA_SCHEMA.getFields())
            .addInt64Field(DEFAULT_SEQ_COL)
            .addStringField("source")
            .build();

    // No change_type_column: change kinds come only from each element's native ValueKind.
    SchemaTransform transform =
        transformFor(
            configFor(id).setMode("merge-on-read").setDrop(ImmutableList.of("source")).build());

    PCollection<Row> input =
        withKinds(
                p.apply(
                    Create.of(
                        ImmutableList.of(
                            KV.of(ValueKind.INSERT, kindRow(inputSchema, 1, "a", "x", 1L)),
                            KV.of(ValueKind.INSERT, kindRow(inputSchema, 2, "b", "y", 1L)),
                            KV.of(ValueKind.UPDATE_BEFORE, kindRow(inputSchema, 1, "a", "x", 2L)),
                            KV.of(ValueKind.UPDATE_AFTER, kindRow(inputSchema, 1, "a", "z", 2L)),
                            KV.of(ValueKind.DELETE, kindRow(inputSchema, 2, "b", "y", 2L))))))
            .setRowSchema(inputSchema);

    PCollectionRowTuple.of(INPUT_TAG, input).apply("CdcWrite", transform);
    p.run().waitUntilFinish();

    // id=1 updated, id=2 deleted , possible only if the native kinds survived the ParDo.
    assertThat(readRows(table), equalTo(ImmutableList.of("1:a:z")));
  }

  /** Mirrors {@code cdc.sink.CdcSinkTestUtils#withKinds} (package-private there). */
  private static PCollection<Row> withKinds(PCollection<KV<ValueKind, Row>> tagged) {
    return tagged.apply(
        "AttachKinds",
        ParDo.of(
            new DoFn<KV<ValueKind, Row>, Row>() {
              @ProcessElement
              public void process(@Element KV<ValueKind, Row> e, OutputReceiver<Row> out) {
                out.builder(e.getValue()).setValueKind(e.getKey()).output();
              }
            }));
  }

  private static Row kindRow(Schema schema, int id, String name, String data, long seq) {
    return Row.withSchema(schema).addValues(id, name, data, seq, "src").build();
  }

  /**
   * The {@code only} projection extracts a nested payload row (the Debezium {@code after} pattern)
   * while carrying the top-level control columns through to the sink.
   */
  @Test
  public void onlyProjectionExtractsPayloadAndPreservesControlColumns() {
    TableIdentifier id = v2Table();
    Table table = catalog.loadTable(id);

    Schema envelopeSchema = envelopeSchema(/* nullablePayload= */ false);

    runCdcWrite(
        configFor(id)
            .setOnly("after")
            .setMode("merge-on-read")
            .setChangeTypeColumn("op")
            .setChangeTypeMap(ImmutableMap.of("c", "INSERT", "u", "UPDATE_AFTER", "d", "DELETE"))
            .setSequenceNumberColumn("seq")
            .setUpsert(true)
            .build(),
        envelopeSchema,
        envelopeRow(envelopeSchema, 1, "a", "x", "c", 1L),
        envelopeRow(envelopeSchema, 2, "b", "y", "c", 1L),
        envelopeRow(envelopeSchema, 1, "a", "z", "u", 2L),
        envelopeRow(envelopeSchema, 2, "b", "y", "d", 2L));

    // "u" upserted id=1, "d" deleted id=2; ts_ms and op never reached the table.
    assertThat(readRows(table), equalTo(ImmutableList.of("1:a:z")));
  }

  /** A Debezium-ish envelope: {@code after} payload row + {@code op}/{@code seq}/{@code ts_ms}. */
  private static Schema envelopeSchema(boolean nullablePayload) {
    Schema.Builder builder = Schema.builder();
    if (nullablePayload) {
      builder.addNullableField("after", Schema.FieldType.row(DATA_SCHEMA));
    } else {
      builder.addRowField("after", DATA_SCHEMA);
    }
    return builder.addStringField("op").addInt64Field("seq").addInt64Field("ts_ms").build();
  }

  private static Row envelopeRow(
      Schema envelopeSchema, int id, String name, String data, String op, long seq) {
    return Row.withSchema(envelopeSchema)
        .addValues(Row.withSchema(DATA_SCHEMA).addValues(id, name, data).build(), op, seq, 1234L)
        .build();
  }

  /**
   * {@code drop} removes extra Debezium-ish envelope columns on the templated path too, so the
   * write succeeds and only the data columns land.
   */
  @Test
  public void dynamicDestinationDropsExtraEnvelopeColumns() {
    String suffix = "t" + System.nanoTime();
    List<Table> tables = templateTables(suffix);

    // Input carries two extra Debezium envelope columns not present in the table.
    Schema envelopeSchema =
        Schema.builder()
            .addFields(INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ.getFields())
            .addInt64Field("ts_ms")
            .addStringField("source")
            .build();

    runCdcWrite(
        templateConfig().setDrop(ImmutableList.of("ts_ms", "source")).build(),
        envelopeSchema,
        Row.withSchema(envelopeSchema)
            .addValues(1, "a_" + suffix, "x", "INSERT", 1L, 1234L, "mysql")
            .build(),
        Row.withSchema(envelopeSchema)
            .addValues(2, "b_" + suffix, "y", "INSERT", 1L, 5678L, "postgres")
            .build());

    // The extra envelope columns were dropped; only id:name:data landed.
    assertThat(readRows(tables.get(0)), equalTo(ImmutableList.of("1:a_" + suffix + ":x")));
    assertThat(readRows(tables.get(1)), equalTo(ImmutableList.of("2:b_" + suffix + ":y")));
  }

  // ---------------------------------------------------------------------------------------------
  // Destinations: single table and dynamic templates
  // ---------------------------------------------------------------------------------------------

  /**
   * A destination template routes by a data column, and each table sees its own changes applied:
   * the sink consumes both control columns, so neither leaks into the written data.
   */
  @Test
  public void dynamicDestinationTemplateRoutesAndAppliesChanges() {
    String suffix = "t" + System.nanoTime();
    List<Table> tables = templateTables(suffix);

    PCollectionRowTuple output =
        applyCdcWrite(
            templateConfig().build(),
            INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ,
            rowWithSeq(1, "a_" + suffix, "x", "INSERT", 1L),
            rowWithSeq(2, "b_" + suffix, "y", "INSERT", 1L),
            rowWithSeq(1, "a_" + suffix, "z", "UPDATE_AFTER", 2L));
    assertTrue(output.has(SNAPSHOTS_TAG));
    assertTrue(output.has(DEAD_LETTER_TAG));

    p.run().waitUntilFinish();

    // id=1 reflects the seq=2 UPDATE_AFTER; no control column leaked into either table.
    assertThat(readRows(tables.get(0)), equalTo(ImmutableList.of("1:a_" + suffix + ":z")));
    assertThat(readRows(tables.get(1)), equalTo(ImmutableList.of("2:b_" + suffix + ":y")));
  }

  /**
   * A destination template may reference a field the projection drops: routing sees the raw record
   * while the written row is the filtered one, so a routing-only column never lands in the table.
   */
  @Test
  public void templateRoutesByDroppedField() {
    String suffix = "t" + System.nanoTime();
    List<Table> tables = templateTables(suffix);
    Schema inputSchema =
        Schema.builder()
            .addFields(INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ.getFields())
            .addStringField("source")
            .build();

    runCdcWrite(
        templateConfig().setTable("db.{source}").setDrop(ImmutableList.of("source")).build(),
        inputSchema,
        Row.withSchema(inputSchema).addValues(1, "a", "x", "INSERT", 1L, "a_" + suffix).build(),
        Row.withSchema(inputSchema).addValues(2, "b", "y", "INSERT", 1L, "b_" + suffix).build());

    assertThat(readRows(tables.get(0)), equalTo(ImmutableList.of("1:a:x")));
    assertThat(readRows(tables.get(1)), equalTo(ImmutableList.of("2:b:y")));
  }

  // ---------------------------------------------------------------------------------------------
  // Change kinds and defaults
  // ---------------------------------------------------------------------------------------------

  @Test
  public void schemaTransformAppliesCdcEndToEnd() {
    TableIdentifier id = v2Table();
    Table table = catalog.loadTable(id);

    PCollectionRowTuple output =
        applyCdcWrite(
            cdcConfigFor(id).build(),
            INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ,
            rowWithSeq(1, "a", "x", "INSERT", 1L),
            rowWithSeq(2, "b", "y", "INSERT", 1L),
            rowWithSeq(1, "a", "x", "UPDATE_BEFORE", 2L),
            rowWithSeq(1, "a", "z", "UPDATE_AFTER", 2L),
            rowWithSeq(2, "b", "y", "DELETE", 2L));

    // Output tags are pinned: snapshots + dead_letter, and no error output when error_handling
    // is not configured.
    assertTrue(output.has(SNAPSHOTS_TAG));
    assertTrue(output.has(DEAD_LETTER_TAG));
    assertFalse(output.has("errors"));

    PAssert.that(output.get(DEAD_LETTER_TAG)).empty();

    p.run().waitUntilFinish();

    // id=1 updated to (a,z); id=2 deleted.
    assertThat(readRows(table), equalTo(ImmutableList.of("1:a:z")));
  }

  // ---------------------------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------------------------

  /**
   * With {@code error_handling}, an invalid record (unknown change-type value) is routed to the
   * configured named output; the good records commit.
   */
  @Test
  public void errorHandlingRoutesInvalidRecords() {
    TableIdentifier id = v2Table();
    Table table = catalog.loadTable(id);

    PCollectionRowTuple output =
        applyCdcWrite(
            cdcConfigFor(id)
                .setErrorHandling(ErrorHandling.builder().setOutput("errors").build())
                .build(),
            INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ,
            rowWithSeq(1, "a", "x", "INSERT", 1L),
            rowWithSeq(2, "b", "y", "BOGUS", 1L),
            rowWithSeq(3, "c", "z", "INSERT", 1L));
    assertTrue(output.has("errors"));
    assertTrue(output.has(SNAPSHOTS_TAG));
    assertTrue(output.has(DEAD_LETTER_TAG));

    PAssert.that(output.get("errors"))
        .satisfies(
            rows -> {
              Row err = ImmutableList.copyOf(rows).get(0);
              Row failedRow = err.getRow("failed_row");
              assertNotNull(failedRow);
              assertEquals(Integer.valueOf(2), failedRow.getInt32("id"));
              return null;
            });

    p.run().waitUntilFinish();

    assertThat(readRows(table), containsInAnyOrder("1:a:x", "3:c:z"));
  }

  // ---------------------------------------------------------------------------------------------
  // Managed
  // ---------------------------------------------------------------------------------------------

  /**
   * {@code Managed.write(Managed.ICEBERG)} with a {@code cdc} map writes CDC end-to-end through the
   * snake_case config surface.
   */
  @Test
  public void managedIcebergMergeOnReadWritesEndToEnd() {
    TableIdentifier id = v2Table();
    Table table = catalog.loadTable(id);

    Map<String, Object> configMap =
        ImmutableMap.<String, Object>builder()
            .put("table", id.toString())
            .put("catalog_properties", catalogProperties())
            .put("mode", "merge-on-read")
            .put("change_type_column", "change_type")
            .build();

    PCollection<Row> input =
        p.apply(
                Create.of(
                    rowWithSeq(1, "a", "x", "INSERT", 1L),
                    rowWithSeq(1, "a", "x", "UPDATE_BEFORE", 2L),
                    rowWithSeq(1, "a", "z", "UPDATE_AFTER", 2L)))
            .setRowSchema(INPUT_SCHEMA_WITH_CHANGE_TYPE_AND_SEQ);

    PCollectionRowTuple output = input.apply(Managed.write(Managed.ICEBERG).withConfig(configMap));
    assertTrue(output.has(SNAPSHOTS_TAG));
    assertTrue(output.has(DEAD_LETTER_TAG));

    p.run().waitUntilFinish();

    assertThat(readRows(table), equalTo(ImmutableList.of("1:a:z")));
  }
}
