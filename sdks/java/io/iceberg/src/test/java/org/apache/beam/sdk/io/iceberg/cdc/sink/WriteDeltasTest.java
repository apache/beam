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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link WriteDeltas}, stage 3 of the CDC sink. The partition tests pin the headline
 * property: partition values ride from live {@link DataFile}s into {@link SerializableDataFile}'s
 * typed JSON and back unchanged, so transforms a path-rendered wire format cannot round-trip are
 * simply legal; each reconstructs the emitted metadata, hand-commits it, and reads the table back
 * as ground truth.
 */
@RunWith(JUnit4.class)
public class WriteDeltasTest {

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  /** Canonical test table schema. */
  private static final Schema ICEBERG_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static final String SINK_ID = "test-sink";
  private static final long BIG_TARGET_FILE_SIZE = 512L * 1024 * 1024;

  /** Transform outputs collected per test; static because the runner serializes DoFn fields. */
  private static final ConcurrentMap<String, List<ShardDeltaFiles>> COLLECTED =
      new ConcurrentHashMap<>();

  private File warehouseDir;
  private Catalog catalog;

  @Before
  public void setUp() throws IOException {
    warehouseDir = tmp.newFolder("warehouse");
    catalog = CdcSinkTestUtils.hadoopCatalog(warehouseDir);
  }

  private IcebergCatalogConfig catalogConfig() {
    return CdcSinkTestUtils.catalogConfig(warehouseDir);
  }

  private static CdcWriteConfig cfg() {
    return CdcWriteConfig.builder().setSinkId(SINK_ID).build();
  }

  private WriteDeltas transform(TableIdentifier id, org.apache.beam.sdk.schemas.Schema dataSchema) {
    return new WriteDeltas(
        catalogConfig(), cfg(), DynamicDestinations.singleTable(id, dataSchema), "px");
  }

  private static Coder<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> groupCoder(
      org.apache.beam.sdk.schemas.Schema dataSchema) {
    return KvCoder.of(
        KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
        IterableCoder.of(KvCoder.of(ByteArrayCoder.of(), CdcRecordCoder.of(dataSchema))));
  }

  /** The {@code id INT32} schema whose encoding keys the sort prefixes below. */
  private static final org.apache.beam.sdk.schemas.Schema PK_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder().addInt32Field("id").build();

  /** {@code pkBytes} for one {@code id}, matching the sink's encoding for an id-keyed table. */
  private static byte[] pkBytes(int id) {
    try {
      return CoderUtils.encodeToByteArray(
          RowCoder.of(PK_SCHEMA), Row.withSchema(PK_SCHEMA).addValues(id).build());
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A sorted-group element: the {@link CdcSortKey} bytes paired with the {@link CdcRecord}. The
   * collapse writer blocks on the key's pk prefix, so it carries the row's encoded {@code id};
   * every fixture here keys rows by a distinct {@code id}.
   */
  private static KV<byte[], CdcRecord> kv(Row data, long seq, ValueKind kind) {
    byte[] pk = pkBytes(checkStateNotNull(data.getInt32("id")));
    return KV.of(CdcSortKey.encode(pk, seq, kind), CdcRecord.of(data, kind, seq));
  }

  private static Row row(org.apache.beam.sdk.schemas.Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  /** Runs the given groups through {@link WriteDeltas} and returns the emitted elements. */
  private List<ShardDeltaFiles> runAndCollect(
      TableIdentifier id,
      org.apache.beam.sdk.schemas.Schema dataSchema,
      List<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> groups) {
    String collectKey = id + "-" + System.nanoTime();
    COLLECTED.put(collectKey, Collections.synchronizedList(new ArrayList<>()));
    p.apply(Create.of(groups).withCoder(groupCoder(dataSchema)))
        .apply(transform(id, dataSchema))
        .apply(ParDo.of(new CollectFn(collectKey)));
    p.run().waitUntilFinish();
    return checkStateNotNull(COLLECTED.get(collectKey));
  }

  /** Collects the transform's output into {@link #COLLECTED} under {@code collectKey}. */
  private static final class CollectFn extends DoFn<ShardDeltaFiles, Void> {
    private final String collectKey;

    CollectFn(String collectKey) {
      this.collectKey = collectKey;
    }

    @ProcessElement
    public void process(@Element ShardDeltaFiles files) {
      checkStateNotNull(COLLECTED.get(collectKey)).add(files);
    }
  }

  /** Reconstructs the live {@link DataFile}s carried by {@code files}, against {@code table}. */
  private static List<DataFile> dataFilesOf(Table table, ShardDeltaFiles files) {
    List<DataFile> reconstructed = new ArrayList<>();
    for (SerializableDataFile f : files.getDataFiles()) {
      reconstructed.add(f.createDataFile(table.specs()));
    }
    return reconstructed;
  }

  /** Reconstructs the live {@link DeleteFile}s carried by {@code files}, against {@code table}. */
  private static List<DeleteFile> deleteFilesOf(Table table, ShardDeltaFiles files) {
    List<DeleteFile> reconstructed = new ArrayList<>();
    for (SerializableDeleteFile f : files.getDeleteFiles()) {
      reconstructed.add(f.createDeleteFile(table.specs(), table.sortOrders()));
    }
    return reconstructed;
  }

  /** All regular files under {@code root}, skipping hidden files (Hadoop's .crc checksums). */
  private static List<File> filesUnder(File root) {
    List<File> found = new ArrayList<>();
    File[] children = root.listFiles();
    if (children == null) {
      return found;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        found.addAll(filesUnder(child));
      } else if (!child.getName().startsWith(".")) {
        found.add(child);
      }
    }
    return found;
  }

  private File tableDir(TableIdentifier id) {
    return new File(warehouseDir, "db/" + id.name());
  }

  /** All data/delete files under the table location, excluding the {@code metadata} directory. */
  private List<File> dataFilesUnder(TableIdentifier id) {
    List<File> found = new ArrayList<>();
    File[] children = tableDir(id).listFiles();
    if (children == null) {
      return found;
    }
    for (File child : children) {
      if (child.isDirectory() && !child.getName().equals("metadata")) {
        found.addAll(filesUnder(child));
      }
    }
    return found;
  }

  // ---------------------------------------------------------------------------------------------
  // 1. + 8. One INSERT group: the data file's metadata is emitted; min/max sequence recorded.
  // ---------------------------------------------------------------------------------------------

  /** Creates the canonical unpartitioned V2 {@link #ICEBERG_SCHEMA} table (PK {@code id}). */
  private Table v2Table(TableIdentifier id) {
    return CdcSinkTestUtils.createTable(
        catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
  }

  @Test
  public void writesAndEmitsDeltaForSortedInsertGroup() {
    TableIdentifier id = TableIdentifier.of("db", "t1_" + System.nanoTime());
    Table t = v2Table(id);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(t.schema());

    Iterable<KV<byte[], CdcRecord>> sorted =
        ImmutableList.of(
            kv(row(dataSchema, 1, "a", "x"), 3L, ValueKind.INSERT),
            kv(row(dataSchema, 2, "b", "y"), 5L, ValueKind.INSERT),
            kv(row(dataSchema, 3, "c", "z"), 9L, ValueKind.INSERT));

    List<ShardDeltaFiles> out =
        runAndCollect(id, dataSchema, ImmutableList.of(KV.of(KV.of(id.toString(), 0), sorted)));

    assertThat(out, hasSize(1));
    ShardDeltaFiles files = out.get(0);
    assertThat(files.getTableIdentifierString(), equalTo(id.toString()));
    assertThat(files.getMinSequenceNumber(), equalTo(3L));
    assertThat(files.getMaxSequenceNumber(), equalTo(9L));
    assertThat(files.getDataFiles(), hasSize(1));
    // INSERT-only, non-upsert: no delete files.
    assertThat(files.getDeleteFiles(), empty());

    List<DataFile> reconstructed = dataFilesOf(t, files);
    assertThat(reconstructed, hasSize(1));
    DataFile dataFile = reconstructed.get(0);
    assertThat(dataFile.recordCount(), equalTo(3L));
    assertThat(dataFile.location(), startsWith(t.location()));
    assertThat(new File(dataFile.location().replaceFirst("^file:", "")).exists(), is(true));
    // No sequence number carried: the eventual commit assigns one by snapshot inheritance.
    assertThat(dataFile.dataSequenceNumber(), nullValue());
  }

  // ---------------------------------------------------------------------------------------------
  // 2. An update pair: the equality-delete file's metadata survives serialization.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void updatePairCarriesEqualityDeleteThroughSerialization() {
    TableIdentifier id = TableIdentifier.of("db", "t2_" + System.nanoTime());
    Table t = v2Table(id);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(t.schema());

    Iterable<KV<byte[], CdcRecord>> sorted =
        ImmutableList.of(
            kv(row(dataSchema, 1, "a", "x"), 5L, ValueKind.UPDATE_BEFORE),
            kv(row(dataSchema, 1, "a2", "x2"), 5L, ValueKind.UPDATE_AFTER),
            kv(row(dataSchema, 2, "b", "y"), 7L, ValueKind.INSERT));

    List<ShardDeltaFiles> out =
        runAndCollect(id, dataSchema, ImmutableList.of(KV.of(KV.of(id.toString(), 0), sorted)));

    assertThat(out, hasSize(1));
    ShardDeltaFiles files = out.get(0);
    assertThat(files.getMinSequenceNumber(), equalTo(5L));
    assertThat(files.getMaxSequenceNumber(), equalTo(7L));

    List<DataFile> dataFiles = dataFilesOf(t, files);
    assertThat(dataFiles, hasSize(1));
    assertThat(dataFiles.get(0).recordCount(), equalTo(2L));

    // The update pair collapsed to one PK-only equality delete row plus the after-image.
    List<DeleteFile> deleteFiles = deleteFilesOf(t, files);
    assertThat(deleteFiles, hasSize(1));
    DeleteFile delete = deleteFiles.get(0);
    assertThat(delete.content(), equalTo(FileContent.EQUALITY_DELETES));
    assertThat(delete.equalityFieldIds(), equalTo(ImmutableList.of(1)));
    assertThat(delete.recordCount(), equalTo(1L));
    // No sequence number carried: the eventual commit assigns one by snapshot inheritance.
    assertThat(delete.dataSequenceNumber(), nullValue());
  }

  // ---------------------------------------------------------------------------------------------
  // 3. day(timestamptz) partition, end-to-end (the predecessor crashed at commit until DATE got a
  //    renderer special-case; typed JSON removes the rendered path entirely).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void dayPartitionedTimestamptzTableCommitsAndReadsBack() throws IOException {
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "event_ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(3, "name", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(icebergSchema).day("event_ts").build();
    TableIdentifier id = TableIdentifier.of("db", "t3_" + System.nanoTime());
    // event_ts is also an equality column: the key-derived-partition shape.
    Table t =
        CdcSinkTestUtils.createTable(catalog, id, icebergSchema, ImmutableSet.of(1, 2), 2, spec);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(t.schema());

    Instant ts1 = Instant.parse("2024-03-15T10:30:00.123456Z");
    Instant ts2 = Instant.parse("2024-06-01T00:00:00Z");
    Iterable<KV<byte[], CdcRecord>> sorted =
        ImmutableList.of(
            kv(row(dataSchema, 1, ts1, "a"), 1L, ValueKind.INSERT),
            kv(row(dataSchema, 2, ts2, "b"), 2L, ValueKind.INSERT));

    List<ShardDeltaFiles> out =
        runAndCollect(id, dataSchema, ImmutableList.of(KV.of(KV.of(id.toString(), 0), sorted)));

    assertThat(out, hasSize(1));
    List<DataFile> files = dataFilesOf(t, out.get(0));
    assertThat(files, hasSize(2));
    Set<Object> partitionValues = new HashSet<>();
    for (DataFile f : files) {
      partitionValues.add(f.partition().get(0, Object.class));
    }
    assertThat(
        partitionValues,
        containsInAnyOrder(
            (int) LocalDate.of(2024, 3, 15).toEpochDay(),
            (int) LocalDate.of(2024, 6, 1).toEpochDay()));

    // Hand-commit the reconstructed files and read the table back as ground truth.
    commitDataFiles(t, files);

    Map<Integer, Record> byId = readById(t);
    assertThat(byId.keySet(), hasSize(2));
    Record row1 = checkStateNotNull(byId.get(1));
    Record row2 = checkStateNotNull(byId.get(2));
    assertThat(row1.getField("event_ts"), equalTo(OffsetDateTime.ofInstant(ts1, ZoneOffset.UTC)));
    assertThat(row1.getField("name"), equalTo("a"));
    assertThat(row2.getField("event_ts"), equalTo(OffsetDateTime.ofInstant(ts2, ZoneOffset.UTC)));
    assertThat(row2.getField("name"), equalTo("b"));
  }

  // ---------------------------------------------------------------------------------------------
  // 4. identity(DATE) partition, end-to-end: DATE is the result type the old design had to
  //    special-case, so it is the sharpest check that partition values travel natively.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void identityDatePartitionedTableCommitsAndReadsBack() throws IOException {
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "event_date", Types.DateType.get()),
            Types.NestedField.optional(3, "name", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(icebergSchema).identity("event_date").build();
    TableIdentifier id = TableIdentifier.of("db", "t4_" + System.nanoTime());
    Table t =
        CdcSinkTestUtils.createTable(catalog, id, icebergSchema, ImmutableSet.of(1, 2), 2, spec);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(t.schema());

    LocalDate d1 = LocalDate.of(2024, 3, 15);
    LocalDate d2 = LocalDate.of(2024, 6, 1);
    Iterable<KV<byte[], CdcRecord>> sorted =
        ImmutableList.of(
            kv(row(dataSchema, 1, d1, "a"), 1L, ValueKind.INSERT),
            kv(row(dataSchema, 2, d2, "b"), 2L, ValueKind.INSERT));

    List<ShardDeltaFiles> out =
        runAndCollect(id, dataSchema, ImmutableList.of(KV.of(KV.of(id.toString(), 0), sorted)));

    assertThat(out, hasSize(1));
    List<DataFile> files = dataFilesOf(t, out.get(0));
    assertThat(files, hasSize(2));
    Set<Object> partitionValues = new HashSet<>();
    for (DataFile f : files) {
      partitionValues.add(f.partition().get(0, Object.class));
    }
    assertThat(partitionValues, containsInAnyOrder((int) d1.toEpochDay(), (int) d2.toEpochDay()));

    commitDataFiles(t, files);

    Map<Integer, Record> byId = readById(t);
    assertThat(byId.keySet(), hasSize(2));
    assertThat(checkStateNotNull(byId.get(1)).getField("event_date"), equalTo(d1));
    assertThat(checkStateNotNull(byId.get(2)).getField("event_date"), equalTo(d2));
  }

  // ---------------------------------------------------------------------------------------------
  // 5. A '/' inside a STRING identity partition value, end-to-end (previously rejected loudly).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void slashInStringPartitionValueRoundTripsEndToEnd() throws IOException {
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(icebergSchema).identity("name").build();
    TableIdentifier id = TableIdentifier.of("db", "t5_" + System.nanoTime());
    Table t =
        CdcSinkTestUtils.createTable(catalog, id, icebergSchema, ImmutableSet.of(1, 2), 2, spec);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(t.schema());

    Iterable<KV<byte[], CdcRecord>> sorted =
        ImmutableList.of(kv(row(dataSchema, 1, "a/b", "x"), 1L, ValueKind.INSERT));

    List<ShardDeltaFiles> out =
        runAndCollect(id, dataSchema, ImmutableList.of(KV.of(KV.of(id.toString(), 0), sorted)));

    assertThat(out, hasSize(1));
    List<DataFile> files = dataFilesOf(t, out.get(0));
    assertThat(files, hasSize(1));
    // The raw partition value survives, slash and all. A partition-PATH round-trip would split
    // "name=a/b" into two fields here; the typed JSON partition has no such hazard.
    assertThat(files.get(0).partition().get(0, String.class), equalTo("a/b"));

    commitDataFiles(t, files);

    Map<Integer, Record> byId = readById(t);
    assertThat(byId.keySet(), hasSize(1));
    Record slashRow = checkStateNotNull(byId.get(1));
    assertThat(slashRow.getField("name"), equalTo("a/b"));
    assertThat(slashRow.getField("data"), equalTo("x"));
  }

  // ---------------------------------------------------------------------------------------------
  // 6. Spec evolution: each file is serialized under the FILES' spec, not the current table spec.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void serializesFilesUnderTheirOwnSpecAfterSpecEvolution() throws IOException {
    TableIdentifier id = TableIdentifier.of("db", "t6_" + System.nanoTime());
    PartitionSpec spec0 = PartitionSpec.builderFor(ICEBERG_SCHEMA).bucket("id", 4).build();
    Table t =
        CdcSinkTestUtils.createTable(catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 2, spec0);
    assertThat(t.spec().specId(), equalTo(0));

    // Write a real data file under spec 0 through the production writer path.
    RecordDeltaTaskWriter writer =
        CdcSinkTestUtils.deltaWriter(t, ImmutableSet.of(1), false, BIG_TARGET_FILE_SIZE);
    GenericRecord rec = GenericRecord.create(t.schema());
    rec.setField("id", 1);
    rec.setField("name", "a");
    rec.setField("data", "x");
    writer.write(CdcSortKey.encode(pkBytes(1), 1L, ValueKind.INSERT), rec, ValueKind.INSERT);
    WriteResult result = writer.complete();
    assertThat(result.dataFiles().length, equalTo(1));
    assertThat(result.dataFiles()[0].specId(), equalTo(0));
    Object originalPartition = result.dataFiles()[0].partition().get(0, Object.class);

    // Mid-run partition-spec evolution: table.spec() is now spec 1, the file is still spec 0.
    t.updateSpec().addField(Expressions.bucket("name", 2)).commit();
    t.refresh();
    assertThat(t.spec().specId(), equalTo(1));

    ShardDeltaFiles files = WriteDeltas.serialize(id.toString(), t, result, 1L, 1L);

    // Serialized against its OWN spec, never the evolved t.spec(), or the single-field partition
    // tuple would be read as the evolved two-field one.
    List<DataFile> reconstructed = dataFilesOf(t, files);
    assertThat(reconstructed, hasSize(1));
    assertThat(reconstructed.get(0).specId(), equalTo(0));
    assertThat(reconstructed.get(0).partition().size(), equalTo(1));
    assertThat(reconstructed.get(0).partition().get(0, Object.class), equalTo(originalPartition));
    assertThat(reconstructed.get(0).location(), equalTo(result.dataFiles()[0].location()));
  }

  // ---------------------------------------------------------------------------------------------
  // 7. Empty group emits nothing (and writes nothing).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void emptyGroupEmitsNothing() {
    TableIdentifier id = TableIdentifier.of("db", "t7_" + System.nanoTime());
    v2Table(id);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA);

    Iterable<KV<byte[], CdcRecord>> emptyGroup = ImmutableList.of();
    PCollection<ShardDeltaFiles> out =
        p.apply(
                Create.of(KV.of(KV.of(id.toString(), 0), emptyGroup))
                    .withCoder(groupCoder(dataSchema)))
            .apply(transform(id, dataSchema));

    PAssert.that(out).empty();
    p.run().waitUntilFinish();

    assertThat(dataFilesUnder(id), empty());
  }

  // ---------------------------------------------------------------------------------------------
  // 8./9. Abort path. The positive control proves the on-disk scan looks in the right place
  // (valid records leave data files); the abort test then proves a mid-group record failure
  // leaves none.
  // ---------------------------------------------------------------------------------------------

  /**
   * A table whose {@code data} column is {@code decimal(4,2)}, with a tiny target file size so a
   * completed file hits disk at the 1000-row roll check; an over-precision poison value then passes
   * the sink's schema validation and fails only at Parquet serialization.
   */
  private Table decimalDataTable(TableIdentifier id) {
    Schema decimalDataSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "data", Types.DecimalType.of(4, 2)));
    Table t =
        CdcSinkTestUtils.createTable(
            catalog, id, decimalDataSchema, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
    t.updateProperties().set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "1").commit();
    return t;
  }

  private static org.apache.beam.sdk.schemas.Schema decimalDataBeamSchema() {
    return org.apache.beam.sdk.schemas.Schema.builder()
        .addInt32Field("id")
        .addNullableStringField("name")
        .addDecimalField("data")
        .build();
  }

  private static List<KV<byte[], CdcRecord>> thousandValidRecords(
      org.apache.beam.sdk.schemas.Schema schema) {
    List<KV<byte[], CdcRecord>> records = new ArrayList<>();
    for (int i = 1; i <= 1000; i++) {
      records.add(kv(row(schema, i, "n" + i, new BigDecimal("1.23")), i, ValueKind.INSERT));
    }
    return sortedBySortKey(records);
  }

  /** Byte-orders {@code records} by sort key: the order the shuffle sorter actually delivers. */
  private static List<KV<byte[], CdcRecord>> sortedBySortKey(List<KV<byte[], CdcRecord>> records) {
    records.sort(
        (a, b) -> UnsignedBytes.lexicographicalComparator().compare(a.getKey(), b.getKey()));
    return records;
  }

  @Test
  public void successfulGroupLeavesDataFilesOnDisk() {
    TableIdentifier id = TableIdentifier.of("db", "t8ctrl_" + System.nanoTime());
    decimalDataTable(id);
    org.apache.beam.sdk.schemas.Schema dataSchema = decimalDataBeamSchema();

    Iterable<KV<byte[], CdcRecord>> sorted = thousandValidRecords(dataSchema);
    List<ShardDeltaFiles> out =
        runAndCollect(id, dataSchema, ImmutableList.of(KV.of(KV.of(id.toString(), 0), sorted)));

    assertThat(out, hasSize(1));
    assertThat(out.get(0).getDataFiles(), not(empty()));
    // The scan below is exactly the one the abort test asserts EMPTY, so this proves it looks in
    // the right place.
    assertThat(dataFilesUnder(id), not(empty()));
  }

  @Test
  public void abortLeavesNoDataFilesOnRecordFailure() {
    TableIdentifier id = TableIdentifier.of("db", "t9_" + System.nanoTime());
    decimalDataTable(id);
    org.apache.beam.sdk.schemas.Schema dataSchema = decimalDataBeamSchema();

    // 1000 valid records (a completed data file is rolled and flushed to disk before the poison
    // sorts in), plus a poison record: a value too wide for the decimal(4,2) column, failing
    // when serialized.
    List<KV<byte[], CdcRecord>> records = thousandValidRecords(dataSchema);
    records.add(
        kv(row(dataSchema, 1001, "bad", new BigDecimal("123.45")), 1001L, ValueKind.INSERT));
    sortedBySortKey(records);

    p.apply(
            Create.of(KV.of(KV.of(id.toString(), 0), (Iterable<KV<byte[], CdcRecord>>) records))
                .withCoder(groupCoder(dataSchema)))
        .apply(transform(id, dataSchema));

    assertThrows(Exception.class, () -> p.run().waitUntilFinish());

    // The abort deleted the flushed file: a failed group leaves nothing, so a retry starts clean.
    assertThat(dataFilesUnder(id), empty());
  }

  // ---------------------------------------------------------------------------------------------
  // 10. A bundle spanning two partition specs serializes each file under its own spec (a staged
  //     manifest could not represent this at all).
  // ---------------------------------------------------------------------------------------------

  @Test
  public void serializesHeterogeneousSpecBundlePerFile() {
    TableIdentifier id = TableIdentifier.of("db", "t10_" + System.nanoTime());
    Table t = v2Table(id);

    DataFile spec0Data =
        DataFiles.builder(t.spec())
            .withFormat(FileFormat.PARQUET)
            .withPath("/tmp/d-0.parquet")
            .withFileSizeInBytes(100L)
            .withRecordCount(2L)
            .build();
    DeleteFile spec0Delete =
        FileMetadata.deleteFileBuilder(t.spec())
            .ofEqualityDeletes(1)
            .withPath("/tmp/eq-0.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(50L)
            .withRecordCount(1L)
            .build();

    // Evolve the spec so a second, spec-1 delete file can make the bundle heterogeneous.
    t.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    t.refresh();
    // UpdatePartitionSpec generates the new field's name (e.g. "id_bucket_4"); read it back rather
    // than guessing.
    String bucketFieldName = t.spec().fields().get(0).name();
    DeleteFile spec1Delete =
        FileMetadata.deleteFileBuilder(t.spec())
            .ofEqualityDeletes(1)
            .withPath("/tmp/eq-1.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(50L)
            .withRecordCount(1L)
            .withPartitionPath(bucketFieldName + "=1")
            .build();

    WriteResult mixedSpecs =
        WriteResult.builder()
            .addDataFiles(spec0Data)
            .addDeleteFiles(spec0Delete, spec1Delete)
            .build();
    ShardDeltaFiles files = WriteDeltas.serialize(id.toString(), t, mixedSpecs, 1L, 2L);

    // Each file keeps its own spec id, and each reconstructs with the right partition arity.
    assertThat(dataFilesOf(t, files).get(0).specId(), equalTo(0));
    List<DeleteFile> deletes = deleteFilesOf(t, files);
    assertThat(deletes, hasSize(2));
    assertThat(deletes.get(0).specId(), equalTo(0));
    assertThat(deletes.get(0).partition().size(), equalTo(0));
    assertThat(deletes.get(1).specId(), equalTo(1));
    assertThat(deletes.get(1).partition().get(0, Integer.class), equalTo(1));
  }

  // ---------------------------------------------------------------------------------------------
  // 11. Multiple shards of the same window produce independent ShardDeltaFiles.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void multipleShardsSameWindowProduceIndependentFileSets() {
    TableIdentifier id = TableIdentifier.of("db", "t11_" + System.nanoTime());
    Table t = v2Table(id);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(t.schema());

    Iterable<KV<byte[], CdcRecord>> shard0 =
        ImmutableList.of(
            kv(row(dataSchema, 1, "a", "x"), 1L, ValueKind.INSERT),
            kv(row(dataSchema, 2, "b", "y"), 2L, ValueKind.INSERT));
    Iterable<KV<byte[], CdcRecord>> shard1 =
        ImmutableList.of(
            kv(row(dataSchema, 3, "c", "z"), 3L, ValueKind.INSERT),
            kv(row(dataSchema, 4, "d", "w"), 4L, ValueKind.INSERT));

    List<ShardDeltaFiles> out =
        runAndCollect(
            id,
            dataSchema,
            ImmutableList.of(
                KV.of(KV.of(id.toString(), 0), shard0), KV.of(KV.of(id.toString(), 1), shard1)));

    assertThat(out, hasSize(2));
    Set<String> dataFilePaths = new HashSet<>();
    Map<Long, ShardDeltaFiles> byMinSeq = new HashMap<>();
    for (ShardDeltaFiles files : out) {
      assertThat(files.getDataFiles(), hasSize(1));
      List<DataFile> reconstructed = dataFilesOf(t, files);
      assertThat(reconstructed.get(0).recordCount(), equalTo(2L));
      dataFilePaths.add(reconstructed.get(0).location());
      byMinSeq.put(files.getMinSequenceNumber(), files);
    }
    // No cross-talk: distinct data files, each covering only its own shard's sequence range.
    assertThat(dataFilePaths, hasSize(2));
    assertThat(checkStateNotNull(byMinSeq.get(1L)).getMaxSequenceNumber(), equalTo(2L));
    assertThat(checkStateNotNull(byMinSeq.get(3L)).getMaxSequenceNumber(), equalTo(4L));
  }

  // ---------------------------------------------------------------------------------------------
  // 12. Mid-run partition-spec evolution: the write path keeps writing under the pinned spec.
  // ---------------------------------------------------------------------------------------------

  /**
   * After a mid-run spec evolution the same {@code WriteDeltasFn} keeps building writers from the
   * pinned spec, never the live {@code table.spec()}.
   */
  @Test
  public void writePathKeepsWritingUnderPinnedSpecAfterEvolution() throws IOException {
    TableIdentifier id = TableIdentifier.of("db", "spec_evolution_" + System.nanoTime());
    v2Table(id);
    org.apache.beam.sdk.schemas.Schema dataSchema =
        IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA);

    TableSetup setup =
        new TableSetup(
            catalogConfig(), cfg(), DynamicDestinations.singleTable(id, dataSchema), "px");
    WriteDeltas.WriteDeltasFn fn = new WriteDeltas.WriteDeltasFn(setup, cfg(), "px", dataSchema);

    // Resolve (and memoize) the destination against the table's original spec.
    TableSetup.Dest dest = setup.get(id.toString(), dataSchema);
    int resolvedSpecId = dest.spec().specId();

    // The operator evolves the spec; the shared Table instance is refreshed onto it.
    dest.table().updateSpec().addField(Expressions.bucket("id", 4)).commit();
    dest.table().refresh();
    assertThat(dest.table().spec().specId(), not(equalTo(resolvedSpecId)));

    KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> group =
        KV.of(
            KV.of(id.toString(), 0),
            ImmutableList.of(kv(row(dataSchema, 1, "a", "x"), 1L, ValueKind.INSERT)));

    List<ShardDeltaFiles> out = new ArrayList<>();
    fn.process(group, GlobalWindow.INSTANCE, CdcSinkTestUtils.collectInto(out));

    assertThat(out, hasSize(1));
    List<DataFile> files = dataFilesOf(dest.table(), out.get(0));
    assertThat(files, hasSize(1));
    assertThat(files.get(0).specId(), equalTo(resolvedSpecId));
    // The pinned spec is the unpartitioned one: the tuple stays empty despite the live spec.
    assertThat(files.get(0).partition().size(), equalTo(0));
  }

  /** Commits {@code files} to {@code table} as one row delta and refreshes the table. */
  private static void commitDataFiles(Table table, List<DataFile> files) {
    RowDelta rowDelta = table.newRowDelta();
    files.forEach(rowDelta::addRows);
    rowDelta.commit();
    table.refresh();
  }

  /** Reads the table's current rows keyed by {@code id}. */
  private static Map<Integer, Record> readById(Table t) throws IOException {
    Map<Integer, Record> byId = new HashMap<>();
    try (CloseableIterable<Record> reader = IcebergGenerics.read(t).build()) {
      for (Record r : reader) {
        byId.put((Integer) checkStateNotNull(r.getField("id")), r);
      }
    }
    return byId;
  }
}
