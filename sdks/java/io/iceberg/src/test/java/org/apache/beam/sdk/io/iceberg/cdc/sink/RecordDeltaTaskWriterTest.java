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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link RecordDeltaTaskWriter}'s streaming collapse. Each case feeds one sorted group
 * (same-key records contiguous, in (seq, kind) order) and asserts the flush truth table at
 * file-content level: the produced Parquet files are read back row by row, and where a table state
 * matters the result is committed with {@link Table#newRowDelta()} and read via {@link
 * IcebergGenerics}.
 *
 * <p>The writer emits at most one equality delete and one data row per key per group, and never
 * writes position deletes or deletion vectors; same-window churn that cancels out reaches no file
 * at all.
 */
@RunWith(JUnit4.class)
public class RecordDeltaTaskWriterTest {

  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static final long TARGET_FILE_SIZE = 512L * 1024 * 1024;

  private File warehouseDir;
  private Catalog catalog;

  @Before
  public void setUp() throws Exception {
    warehouseDir = tmp.newFolder("warehouse");
    catalog = CdcSinkTestUtils.hadoopCatalog(warehouseDir);
  }

  /** Creates the canonical unpartitioned V2 table with {@code id} as the identifier/PK. */
  private Table v2Table() {
    return CdcSinkTestUtils.createTable(
        catalog,
        TableIdentifier.of("db", "t" + System.nanoTime()),
        SCHEMA,
        ImmutableSet.of(1),
        2,
        PartitionSpec.unpartitioned());
  }

  /** Creates the canonical unpartitioned V3 table with {@code id} as the identifier/PK. */
  private Table v3Table() {
    return CdcSinkTestUtils.createTable(
        catalog,
        TableIdentifier.of("db", "v3_" + System.nanoTime()),
        SCHEMA,
        ImmutableSet.of(1),
        3,
        PartitionSpec.unpartitioned());
  }

  /** Creates a V2 table partitioned by {@code bucket(id, 2)} (the PK). */
  private Table v2BucketPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 2).build();
    return CdcSinkTestUtils.createTable(
        catalog,
        TableIdentifier.of("db", "p" + System.nanoTime()),
        SCHEMA,
        ImmutableSet.of(1),
        2,
        spec);
  }

  /** Creates a V2 table partitioned by {@code identity(name)}, a NON-key column (PK stays id). */
  private Table v2NonKeyPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("name").build();
    return CdcSinkTestUtils.createTable(
        catalog,
        TableIdentifier.of("db", "nk" + System.nanoTime()),
        SCHEMA,
        ImmutableSet.of(1),
        2,
        spec);
  }

  /** A V2 table BORN with a sort order on a non-key column: its only sort order id is 1, not 0. */
  private Table v2SortedTable() {
    return CdcSinkTestUtils.createSortedTable(
        catalog,
        TableIdentifier.of("db", "s" + System.nanoTime()),
        SCHEMA,
        ImmutableSet.of(1),
        2,
        PartitionSpec.unpartitioned(),
        SortOrder.builderFor(SCHEMA).asc("name").build());
  }

  /** A production-path writer with this suite's PK ({@code id}) and target file size. */
  private static RecordDeltaTaskWriter writer(Table t, boolean upsert) {
    return CdcSinkTestUtils.deltaWriter(t, ImmutableSet.of(1), upsert, TARGET_FILE_SIZE);
  }

  private static Record rec(Table t, int id, String name, String data) {
    GenericRecord r = GenericRecord.create(t.schema());
    r.setField("id", id);
    r.setField("name", name);
    r.setField("data", data);
    return r;
  }

  /** Writes one change: the sort key's pk prefix carries the record's encoded {@code id}. */
  private static void write(RecordDeltaTaskWriter w, Record rec, long seq, ValueKind kind) {
    byte[] pk = Ints.toByteArray((Integer) rec.getField("id"));
    w.write(CdcSortKey.encode(pk, seq, kind), rec, kind);
  }

  /** Reads the table's current rows as sorted {@code "id:name:data"} strings. */
  private static List<String> readRows(Table t) throws IOException {
    List<String> rows = new ArrayList<>();
    try (CloseableIterable<Record> reader = IcebergGenerics.read(t).build()) {
      for (Record r : reader) {
        rows.add(r.getField("id") + ":" + r.getField("name") + ":" + r.getField("data"));
      }
    }
    Collections.sort(rows);
    return rows;
  }

  /** Reads a Parquet data/delete file's rows with the given projection (matched by field id). */
  private static List<Record> readParquetRows(Table t, String location, Schema projection)
      throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(t.io().newInputFile(location))
            .project(projection)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema))
            .build()) {
      return ImmutableList.copyOf(reader);
    }
  }

  /** A file's rows as {@code "id:name:data"} strings against the full table schema. */
  private static List<String> readFileRows(Table t, String location) throws IOException {
    return readParquetRows(t, location, t.schema()).stream()
        .map(r -> r.getField("id") + ":" + r.getField("name") + ":" + r.getField("data"))
        .collect(Collectors.toList());
  }

  /** The single delete file, asserted to be a PK-only equality delete over the given ids. */
  private static void assertEqualityDeleteOfIds(Table t, WriteResult r, Integer... ids)
      throws IOException {
    assertThat(r.deleteFiles(), arrayWithSize(1));
    DeleteFile del = r.deleteFiles()[0];
    assertThat(del.content(), equalTo(FileContent.EQUALITY_DELETES));
    assertThat(del.equalityFieldIds(), contains(1));

    // PK-only rows: projecting the full schema over the delete file yields nulls for name/data;
    // a full-row equality delete would read the data columns back.
    List<Record> deleteRows = readParquetRows(t, del.location(), t.schema());
    List<Integer> deletedIds = new ArrayList<>();
    for (Record row : deleteRows) {
      deletedIds.add((Integer) row.getField("id"));
      assertThat(row.getField("name"), nullValue());
      assertThat(row.getField("data"), nullValue());
    }
    assertThat(deletedIds, containsInAnyOrder(ids));
  }

  // ---------------------------------------------------------------------------------------------
  // Flush truth table, non-upsert
  // ---------------------------------------------------------------------------------------------

  // [I] -> row only.
  @Test
  public void insertWritesDataFileOnlyAndReadsBack() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    write(w, rec(t, 2, "b", "y"), 1L, ValueKind.INSERT);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(r.deleteFiles(), emptyArray());
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:a:x", "2:b:y"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:a:x", "2:b:y"));
  }

  // [I, D] -> nothing: the key was born and died this window, so no file is written at all.
  @Test
  public void insertThenDeleteEmitsNothing() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    write(w, rec(t, 1, "a", "x"), 2L, ValueKind.DELETE);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), emptyArray());
    assertThat(r.deleteFiles(), emptyArray());
    assertThat(dataFilesUnder(warehouseDir), empty());
  }

  // [I, UB, UA] -> row only: born this window, so its churn needs no delete.
  @Test
  public void insertUpdatedInWindowWritesFinalRowOnly() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    write(w, rec(t, 1, "a", "x"), 2L, ValueKind.UPDATE_BEFORE);
    write(w, rec(t, 1, "a2", "x2"), 2L, ValueKind.UPDATE_AFTER);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(r.deleteFiles(), emptyArray());
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:a2:x2"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:a2:x2"));
  }

  // [UB, UA] -> delete + row, reaching a row committed by an earlier writer.
  @Test
  public void updatePairWritesEqualityDeleteAndFinalRow() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "old", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "old", "x"), 2L, ValueKind.UPDATE_BEFORE);
    write(b, rec(t, 1, "new", "y"), 2L, ValueKind.UPDATE_AFTER);
    WriteResult r = b.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:new:y"));
    assertEqualityDeleteOfIds(t, r, 1);

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:new:y"));
  }

  // [D, I] with an earlier committed row -> delete + row: the delete survives the block ending in
  // INSERT (a reinsert must still remove the committed image).
  @Test
  public void deleteThenReinsertReplacesTheCommittedRow() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "old", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "old", "x"), 2L, ValueKind.DELETE);
    write(b, rec(t, 1, "new", "y"), 3L, ValueKind.INSERT);
    WriteResult r = b.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:new:y"));
    assertEqualityDeleteOfIds(t, r, 1);

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:new:y"));
  }

  // [D] -> delete only, removing a row committed by an earlier writer.
  @Test
  public void deleteWritesPkOnlyEqualityDeleteOnly() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "a", "x"), 2L, ValueKind.DELETE);
    WriteResult r = b.complete();

    assertThat(r.dataFiles(), emptyArray());
    assertEqualityDeleteOfIds(t, r, 1);

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), empty());
  }

  // [UA] -> row only: bare-UA parity, a lone after-image writes without deleting.
  @Test
  public void bareUpdateAfterWritesRowWithoutDelete() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a2", "x2"), 2L, ValueKind.UPDATE_AFTER);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(r.deleteFiles(), emptyArray());
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:a2:x2"));
    w.abort();
  }

  // [UA, UB, UA] -> delete + row: the opening UA fails the sawUbOrDelete arm only until the UB.
  @Test
  public void updateAfterChurnEndingInUpdateAfterWritesDeleteAndRow() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a2", "x2"), 2L, ValueKind.UPDATE_AFTER);
    write(w, rec(t, 1, "a2", "x2"), 3L, ValueKind.UPDATE_BEFORE);
    write(w, rec(t, 1, "a3", "x3"), 3L, ValueKind.UPDATE_AFTER);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:a3:x3"));
    assertEqualityDeleteOfIds(t, r, 1);
    w.abort();
  }

  // [I, I] -> row only, the LAST image: a duplicate insert supersedes the first in the writer.
  @Test
  public void duplicateInsertKeepsLastImage() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    write(w, rec(t, 1, "b", "y"), 2L, ValueKind.INSERT);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(r.deleteFiles(), emptyArray());
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:b:y"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:b:y"));
  }

  // ---------------------------------------------------------------------------------------------
  // Flush truth table, upsert (every block deletes first; UPDATE_BEFOREs are dropped upstream)
  // ---------------------------------------------------------------------------------------------

  // upsert [I] -> delete + row, replacing a previously committed image of the key.
  @Test
  public void upsertInsertWritesDeleteAndRow() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, true /* upsert */);
    write(b, rec(t, 1, "b", "y"), 2L, ValueKind.INSERT);
    WriteResult r = b.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertEqualityDeleteOfIds(t, r, 1);

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:b:y"));
  }

  // upsert [D] -> delete only.
  @Test
  public void upsertDeleteWritesDeleteOnly() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, true /* upsert */);
    write(w, rec(t, 1, "a", "x"), 2L, ValueKind.DELETE);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), emptyArray());
    assertEqualityDeleteOfIds(t, r, 1);
    w.abort();
  }

  // upsert [UA] -> delete + row.
  @Test
  public void upsertUpdateAfterWritesDeleteAndRow() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, true /* upsert */);
    write(w, rec(t, 1, "a2", "x2"), 2L, ValueKind.UPDATE_AFTER);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:a2:x2"));
    assertEqualityDeleteOfIds(t, r, 1);
    w.abort();
  }

  // ---------------------------------------------------------------------------------------------
  // Multi-key groups and partition fanout
  // ---------------------------------------------------------------------------------------------

  // A multi-key group flushes per block: dead key omitted, update pair collapsed, insert written.
  @Test
  public void multiKeyGroupFlushesPerBlock() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 2, "old", "o"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "a", "x"), 2L, ValueKind.INSERT);
    write(b, rec(t, 1, "a", "x"), 3L, ValueKind.DELETE);
    write(b, rec(t, 2, "old", "o"), 2L, ValueKind.UPDATE_BEFORE);
    write(b, rec(t, 2, "new", "n"), 2L, ValueKind.UPDATE_AFTER);
    write(b, rec(t, 3, "c", "z"), 2L, ValueKind.INSERT);
    WriteResult r = b.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("2:new:n", "3:c:z"));
    assertEqualityDeleteOfIds(t, r, 2);

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("2:new:n", "3:c:z"));
  }

  /**
   * Partitioned fanout: the sort is by PK, so partitions interleave (bucket A, B, then A again),
   * and each block's data row and equality delete must land in the block's own partition. The
   * return to bucket A pins fanout: a clustered (one-open-partition) writer would refuse it.
   */
  @Test
  public void partitionedFanoutRoutesRowAndDeleteToTheBlockPartition() throws Exception {
    Table t = v2BucketPartitionedTable();
    SerializableFunction<Integer, Integer> bucketOf =
        Transforms.<Integer>bucket(2).bind(Types.IntegerType.get());
    // Four ascending ids whose buckets go A, B, A, A.
    int firstA = nextIdInBucket(bucketOf, 1, 0);
    int midB = nextIdInBucket(bucketOf, firstA + 1, 1);
    int deadA = nextIdInBucket(bucketOf, midB + 1, 0);
    int lastA = nextIdInBucket(bucketOf, deadA + 1, 0);

    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, midB, "old", "o"), 1L, ValueKind.INSERT);
    write(a, rec(t, deadA, "gone", "g"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, firstA, "a", "x"), 2L, ValueKind.INSERT);
    write(b, rec(t, midB, "old", "o"), 2L, ValueKind.UPDATE_BEFORE);
    write(b, rec(t, midB, "new", "n"), 2L, ValueKind.UPDATE_AFTER);
    write(b, rec(t, deadA, "gone", "g"), 2L, ValueKind.DELETE);
    write(b, rec(t, lastA, "d", "w"), 2L, ValueKind.INSERT);
    WriteResult r = b.complete();

    // One data file per touched bucket; bucket A's holds both of its blocks' rows.
    assertThat(r.dataFiles(), arrayWithSize(2));
    for (DataFile file : r.dataFiles()) {
      Integer bucket = file.partition().get(0, Integer.class);
      List<String> rows = readFileRows(t, file.location());
      if (bucket == 0) {
        assertThat(rows, contains(firstA + ":a:x", lastA + ":d:w"));
      } else {
        assertThat(rows, contains(midB + ":new:n"));
      }
    }

    // One equality delete per touched bucket, in the bucket of the key it removes.
    assertThat(r.deleteFiles(), arrayWithSize(2));
    for (DeleteFile file : r.deleteFiles()) {
      assertThat(file.content(), equalTo(FileContent.EQUALITY_DELETES));
      Integer bucket = file.partition().get(0, Integer.class);
      List<Record> keys = readParquetRows(t, file.location(), t.schema());
      assertThat(keys, hasSize(1));
      int deletedId = (Integer) keys.get(0).getField("id");
      assertThat(deletedId, equalTo(bucket == 1 ? midB : deadA));
    }

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), containsInAnyOrder(firstA + ":a:x", midB + ":new:n", lastA + ":d:w"));
  }

  /** The smallest id at or above {@code from} whose {@code bucketOf} value is {@code bucket}. */
  private static int nextIdInBucket(
      SerializableFunction<Integer, Integer> bucketOf, int from, int bucket) {
    int id = from;
    while (bucketOf.apply(id) != bucket) {
      id++;
    }
    return id;
  }

  // ---------------------------------------------------------------------------------------------
  // Non-key partitioning: the delete routes by the block's OPENING record
  // ---------------------------------------------------------------------------------------------

  /** The single {@code identity(name)} partition value of a data or delete file. */
  private static String partitionOf(ContentFile<?> file) {
    return file.partition().get(0, String.class);
  }

  // [UB(p1), UA(p2)]: the delete lands in the OLD partition, the row in the new one. Routing the
  // delete by the latest record would leave the p1 row alive forever.
  @Test
  public void movedRowDeletesFromOldPartitionAndWritesToNew() throws Exception {
    Table t = v2NonKeyPartitionedTable();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "p1", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "p1", "x"), 2L, ValueKind.UPDATE_BEFORE);
    write(b, rec(t, 1, "p2", "y"), 2L, ValueKind.UPDATE_AFTER);
    WriteResult r = b.complete();

    assertEqualityDeleteOfIds(t, r, 1);
    assertThat(partitionOf(r.deleteFiles()[0]), equalTo("p1"));
    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(partitionOf(r.dataFiles()[0]), equalTo("p2"));
    assertThat(readFileRows(t, r.dataFiles()[0].location()), contains("1:p2:y"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:p2:y"));
  }

  // [UB(p1), UA(p2), UB(p2), UA(p3)]: still one delete, at the OPENING partition p1 (the only one
  // holding a committed row), and one row at the final p3; the p2 stopover reaches no file.
  @Test
  public void multiMoveDeletesOnceAtTheOpeningPartition() throws Exception {
    Table t = v2NonKeyPartitionedTable();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "p1", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "p1", "x"), 2L, ValueKind.UPDATE_BEFORE);
    write(b, rec(t, 1, "p2", "y"), 2L, ValueKind.UPDATE_AFTER);
    write(b, rec(t, 1, "p2", "y"), 3L, ValueKind.UPDATE_BEFORE);
    write(b, rec(t, 1, "p3", "z"), 3L, ValueKind.UPDATE_AFTER);
    WriteResult r = b.complete();

    assertEqualityDeleteOfIds(t, r, 1);
    assertThat(partitionOf(r.deleteFiles()[0]), equalTo("p1"));
    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(partitionOf(r.dataFiles()[0]), equalTo("p3"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:p3:z"));
  }

  // [I(p1), UA(p2)]: born this window, so the move needs no delete; only the p2 row is written.
  @Test
  public void bornThisWindowMoveWritesFinalRowOnly() throws Exception {
    Table t = v2NonKeyPartitionedTable();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "p1", "x"), 1L, ValueKind.INSERT);
    write(w, rec(t, 1, "p2", "y"), 2L, ValueKind.UPDATE_AFTER);
    WriteResult r = w.complete();

    assertThat(r.deleteFiles(), emptyArray());
    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(partitionOf(r.dataFiles()[0]), equalTo("p2"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:p2:y"));
  }

  // [D(p1)]: a DELETE carries the row's actual old values (the input contract), so its equality
  // delete lands in the partition the row occupies.
  @Test
  public void deleteCarryingOldValuesLandsInTheRowsPartition() throws Exception {
    Table t = v2NonKeyPartitionedTable();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "p1", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "p1", "x"), 2L, ValueKind.DELETE);
    WriteResult r = b.complete();

    assertThat(r.dataFiles(), emptyArray());
    assertEqualityDeleteOfIds(t, r, 1);
    assertThat(partitionOf(r.deleteFiles()[0]), equalTo("p1"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), empty());
  }

  // A composite key: the equality delete carries both key columns, and only records agreeing on
  // both collapse into one block.
  @Test
  public void compositeKeyProjectsEveryKeyColumnAndCollapsesPerKey() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "tenant", Types.StringType.get()),
            Types.NestedField.required(2, "id", Types.LongType.get()),
            Types.NestedField.optional(3, "name", Types.StringType.get()),
            Types.NestedField.optional(4, "data", Types.StringType.get()));
    Table t = table(schema, ImmutableSet.of(1, 2));
    RecordDeltaTaskWriter a =
        CdcSinkTestUtils.deltaWriter(t, ImmutableSet.of(1, 2), false, TARGET_FILE_SIZE);
    writeKeyed(a, record(t, "a", 1L, "old", "x"), 0L, ValueKind.INSERT, "a", 1L);
    writeKeyed(a, record(t, "b", 1L, "c", "w"), 0L, ValueKind.INSERT, "b", 1L);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter w =
        CdcSinkTestUtils.deltaWriter(t, ImmutableSet.of(1, 2), false, TARGET_FILE_SIZE);
    writeKeyed(w, record(t, "a", 1L, "old", "x"), 1L, ValueKind.UPDATE_BEFORE, "a", 1L);
    writeKeyed(w, record(t, "a", 1L, "new", "y"), 1L, ValueKind.UPDATE_AFTER, "a", 1L);
    writeKeyed(w, record(t, "a", 2L, "b", "z"), 1L, ValueKind.INSERT, "a", 2L);
    writeKeyed(w, record(t, "b", 1L, "c", "w"), 1L, ValueKind.DELETE, "b", 1L);
    WriteResult r = w.complete();

    assertThat(r.dataFiles(), arrayWithSize(1));
    assertThat(rowStrings(t, r.dataFiles()[0].location()), contains("a:1:new:y", "a:2:b:z"));
    DeleteFile del = singleEqualityDelete(r, 1, 2);
    assertThat(rowStrings(t, del.location()), containsInAnyOrder("a:1:null:null", "b:1:null:null"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t, schema), contains("a:1:new:y", "a:2:b:z"));
  }

  // Non-integer key types round-trip through the PK-only delete file and match on read.
  @Test
  public void keyTypesRoundTripThroughEqualityDeletes() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "k_long", Types.LongType.get()),
            Types.NestedField.required(2, "k_str", Types.StringType.get()),
            Types.NestedField.required(3, "k_date", Types.DateType.get()),
            Types.NestedField.required(4, "k_ts", Types.TimestampType.withZone()),
            Types.NestedField.required(5, "k_dec", Types.DecimalType.of(10, 2)),
            Types.NestedField.required(6, "k_uuid", Types.UUIDType.get()),
            Types.NestedField.optional(7, "payload", Types.StringType.get()));
    ImmutableSet<Integer> keyIds = ImmutableSet.of(1, 2, 3, 4, 5, 6);
    Object[] key = {
      7L,
      "seven",
      LocalDate.of(2026, 9, 3),
      OffsetDateTime.of(2026, 9, 3, 12, 30, 0, 0, ZoneOffset.UTC),
      new BigDecimal("12.34"),
      UUID.randomUUID()
    };
    Table t = table(schema, keyIds);
    RecordDeltaTaskWriter a = CdcSinkTestUtils.deltaWriter(t, keyIds, false, TARGET_FILE_SIZE);
    writeKeyed(a, record(t, concat(key, "old")), 0L, ValueKind.INSERT, key);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter w = CdcSinkTestUtils.deltaWriter(t, keyIds, false, TARGET_FILE_SIZE);
    writeKeyed(w, record(t, concat(key, "old")), 1L, ValueKind.UPDATE_BEFORE, key);
    writeKeyed(w, record(t, concat(key, "new")), 1L, ValueKind.UPDATE_AFTER, key);
    WriteResult r = w.complete();

    DeleteFile del = singleEqualityDelete(r, 1, 2, 3, 4, 5, 6);
    Record deleteRow = Iterables.getOnlyElement(readParquetRows(t, del.location(), schema));
    for (int i = 0; i < key.length; i++) {
      assertThat(schema.columns().get(i).name(), deleteRow.get(i), equalTo(key[i]));
    }
    assertThat(deleteRow.getField("payload"), nullValue());

    CdcSinkTestUtils.commitRowDelta(t, r);
    List<Record> rows = ImmutableList.copyOf(IcebergGenerics.read(t).build());
    assertThat(Iterables.getOnlyElement(rows).getField("payload"), equalTo("new"));
  }

  // The key column is projected by field id, not by position: here it is the last column.
  @Test
  public void keyNotInFirstPositionIsProjectedByFieldId() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.required(3, "id", Types.IntegerType.get()));
    Table t = table(schema, ImmutableSet.of(3));
    RecordDeltaTaskWriter a =
        CdcSinkTestUtils.deltaWriter(t, ImmutableSet.of(3), false, TARGET_FILE_SIZE);
    writeKeyed(a, record(t, "n1", "d1", 7), 0L, ValueKind.INSERT, 7);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter w =
        CdcSinkTestUtils.deltaWriter(t, ImmutableSet.of(3), false, TARGET_FILE_SIZE);
    writeKeyed(w, record(t, "n1", "d1", 7), 1L, ValueKind.UPDATE_BEFORE, 7);
    writeKeyed(w, record(t, "n2", "d2", 7), 1L, ValueKind.UPDATE_AFTER, 7);
    WriteResult r = w.complete();

    assertThat(rowStrings(t, r.dataFiles()[0].location()), contains("n2:d2:7"));
    DeleteFile del = singleEqualityDelete(r, 3);
    assertThat(rowStrings(t, del.location()), contains("null:null:7"));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t, schema), contains("n2:d2:7"));
  }

  private Table table(Schema schema, ImmutableSet<Integer> identifierFieldIds) {
    return CdcSinkTestUtils.createTable(
        catalog,
        TableIdentifier.of("db", "k" + System.nanoTime()),
        schema,
        identifierFieldIds,
        2,
        PartitionSpec.unpartitioned());
  }

  private static Record record(Table t, Object... values) {
    GenericRecord r = GenericRecord.create(t.schema());
    for (int i = 0; i < values.length; i++) {
      r.set(i, values[i]);
    }
    return r;
  }

  private static Object[] concat(Object[] head, Object tail) {
    Object[] all = new Object[head.length + 1];
    System.arraycopy(head, 0, all, 0, head.length);
    all[head.length] = tail;
    return all;
  }

  /** Writes one change whose sort-key pk prefix encodes the given key values. */
  private static void writeKeyed(
      RecordDeltaTaskWriter w, Record rec, long seq, ValueKind kind, Object... key) {
    w.write(CdcSortKey.encode(pkBytes(key), seq, kind), rec, kind);
  }

  /** Length-prefixed string forms: distinct key tuples get distinct, sortable bytes. */
  private static byte[] pkBytes(Object... key) {
    List<byte[]> parts = new ArrayList<>();
    int size = 0;
    for (Object value : key) {
      byte[] part = String.valueOf(value).getBytes(StandardCharsets.UTF_8);
      parts.add(part);
      size += 4 + part.length;
    }
    ByteBuffer buf = ByteBuffer.allocate(size);
    for (byte[] part : parts) {
      buf.putInt(part.length).put(part);
    }
    return buf.array();
  }

  private static DeleteFile singleEqualityDelete(WriteResult r, Integer... equalityFieldIds) {
    assertThat(r.deleteFiles(), arrayWithSize(1));
    DeleteFile del = r.deleteFiles()[0];
    assertThat(del.content(), equalTo(FileContent.EQUALITY_DELETES));
    assertThat(del.equalityFieldIds(), contains(equalityFieldIds));
    return del;
  }

  /** A file's rows as colon-joined field values in table column order. */
  private static List<String> rowStrings(Table t, String location) throws IOException {
    List<String> rows = new ArrayList<>();
    for (Record r : readParquetRows(t, location, t.schema())) {
      rows.add(joined(r, t.schema()));
    }
    return rows;
  }

  /** The table's current rows as sorted colon-joined strings in column order. */
  private static List<String> readRows(Table t, Schema schema) throws IOException {
    List<String> rows = new ArrayList<>();
    try (CloseableIterable<Record> reader = IcebergGenerics.read(t).build()) {
      for (Record r : reader) {
        rows.add(joined(r, schema));
      }
    }
    Collections.sort(rows);
    return rows;
  }

  private static String joined(Record r, Schema schema) {
    List<String> values = new ArrayList<>();
    for (Types.NestedField column : schema.columns()) {
      values.add(String.valueOf(r.getField(column.name())));
    }
    return String.join(":", values);
  }

  // ---------------------------------------------------------------------------------------------
  // Format versions, formats, abort, sort order ids
  // ---------------------------------------------------------------------------------------------

  // V3 gets the identical treatment: cross-commit deletes are Parquet equality deletes.
  @Test
  public void v3UpdatePairWritesParquetEqualityDelete() throws Exception {
    Table t = v3Table();
    RecordDeltaTaskWriter a = writer(t, false);
    write(a, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    CdcSinkTestUtils.commitRowDelta(t, a.complete());

    RecordDeltaTaskWriter b = writer(t, false);
    write(b, rec(t, 1, "a", "x"), 2L, ValueKind.UPDATE_BEFORE);
    write(b, rec(t, 1, "b", "z"), 2L, ValueKind.UPDATE_AFTER);
    WriteResult r = b.complete();

    assertThat(r.deleteFiles(), arrayWithSize(1));
    assertThat(r.deleteFiles()[0].content(), equalTo(FileContent.EQUALITY_DELETES));
    assertThat(r.deleteFiles()[0].format(), equalTo(FileFormat.PARQUET));

    CdcSinkTestUtils.commitRowDelta(t, r);
    assertThat(readRows(t), contains("1:b:z"));
  }

  // An out-of-order pair (seq 2 before seq 1) trips the sort tripwire instead of miscollapsing.
  @Test
  public void unsortedInputThrowsNamingTheProblem() throws Exception {
    Table t = v2Table();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a", "x"), 2L, ValueKind.UPDATE_AFTER);

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> write(w, rec(t, 1, "a", "x"), 1L, ValueKind.UPDATE_BEFORE));

    assertThat(error.getMessage(), containsString("unsorted input"));
    w.abort();
  }

  // abort() after a partial write removes everything it wrote from the filesystem.
  @Test
  public void abortDeletesWrittenFiles() throws Exception {
    Table t = v2Table();
    // Avro materializes files at writer-open (Parquet buffers in memory), so the pre-abort
    // existence check is non-vacuous.
    t.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "avro").commit();
    RecordDeltaTaskWriter w = writer(t, false);
    write(w, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    write(w, rec(t, 2, "b", "y"), 1L, ValueKind.INSERT);

    assertThat(dataFilesUnder(warehouseDir), not(empty()));
    w.abort();
    assertThat(dataFilesUnder(warehouseDir), empty());
  }

  // abort() still deletes every file when closing one of them fails, and surfaces that failure.
  @Test
  public void abortDeletesFilesEvenWhenCloseFails() throws Exception {
    Table t = v2Table();
    t.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "avro").commit();
    CloseFailingFileIO io = new CloseFailingFileIO(t.io());
    FileFormat format = RecordDeltaTaskWriter.dataFileFormat(t);
    RecordDeltaTaskWriter w =
        RecordDeltaTaskWriter.create(
            t,
            t.spec(),
            ImmutableSet.of(1),
            true,
            TARGET_FILE_SIZE,
            OutputFileFactory.builderFor(t, 1, 1).ioSupplier(() -> io).build(),
            format,
            RecordDeltaTaskWriter.deleteFileFormat(t, format));
    // The second key flushes the first block, opening the delete file (fails on close) and the
    // data file (closes fine).
    write(w, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
    write(w, rec(t, 2, "b", "y"), 1L, ValueKind.INSERT);
    assertThat(dataFilesUnder(warehouseDir), not(empty()));

    Exception failure = assertThrows(Exception.class, w::abort);
    assertThat(Throwables.getRootCause(failure).getMessage(), containsString("simulated"));
    assertThat(dataFilesUnder(warehouseDir), empty());
  }

  // Factory format resolution: write.format.default and write.delete.format.default.
  @Test
  public void resolvesDataAndDeleteFileFormats() {
    Table t = v2Table();
    assertThat(RecordDeltaTaskWriter.dataFileFormat(t), equalTo(FileFormat.PARQUET));
    assertThat(
        RecordDeltaTaskWriter.deleteFileFormat(t, FileFormat.PARQUET), equalTo(FileFormat.PARQUET));

    t.updateProperties().set(TableProperties.DELETE_DEFAULT_FILE_FORMAT, "avro").commit();
    assertThat(
        RecordDeltaTaskWriter.deleteFileFormat(t, FileFormat.PARQUET), equalTo(FileFormat.AVRO));
  }

  /**
   * Every equality delete this writer produces carries sort order id 0 (unsorted), INCLUDING on a
   * table that declares a sort order. Pins the premise {@code
   * CommitDeltas.sortOrdersForReconstruction} rests on: if the writer ever stamped the table's real
   * sort order, that special case would become both unnecessary and wrong, and this test says so.
   */
  @Test
  public void sinkEqualityDeletesCarryUnsortedSortOrderId() throws Exception {
    for (Table t : ImmutableList.of(v2Table(), v2SortedTable())) {
      // Writer A commits an INSERT so writer B's DELETE is a cross-commit equality delete.
      RecordDeltaTaskWriter a = writer(t, false);
      write(a, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
      CdcSinkTestUtils.commitRowDelta(t, a.complete());

      RecordDeltaTaskWriter b = writer(t, false);
      write(b, rec(t, 1, "a", "x"), 2L, ValueKind.DELETE);
      DeleteFile[] deletes = b.complete().deleteFiles();

      assertThat(deletes, arrayWithSize(1));
      assertThat(deletes[0].content(), equalTo(FileContent.EQUALITY_DELETES));
      assertThat(deletes[0].sortOrderId(), equalTo(SortOrder.unsorted().orderId()));
      b.abort();
    }
  }

  /**
   * The data-file half of the same premise: sink data files also carry sort order id 0. DO NOT add
   * {@code .dataSortOrder(...)} to the factory: {@code SerializableDataFile} carries no
   * sortOrderId, so a real sort order would be silently RESET at reconstruction, and nothing but
   * this test would notice.
   */
  @Test
  public void sinkDataFilesCarryUnsortedSortOrderId() throws Exception {
    for (Table t : ImmutableList.of(v2Table(), v2SortedTable())) {
      RecordDeltaTaskWriter w = writer(t, false);
      write(w, rec(t, 1, "a", "x"), 1L, ValueKind.INSERT);
      DataFile[] dataFiles = w.complete().dataFiles();

      assertThat(dataFiles, arrayWithSize(1));
      assertThat(dataFiles[0].sortOrderId(), equalTo(SortOrder.unsorted().orderId()));

      // ...and the transport round trip preserves it, which is only true while it IS 0:
      // SerializableDataFile has no sortOrderId field to carry anything else.
      DataFile rebuilt =
          SerializableDataFile.from(dataFiles[0], t.spec()).createDataFile(t.specs());
      assertThat(rebuilt.sortOrderId(), equalTo(dataFiles[0].sortOrderId()));
    }
  }

  /** Regular files under any table's {@code data/} directory (excludes {@code metadata/}). */
  private static List<Path> dataFilesUnder(File dir) throws IOException {
    String dataSegment = File.separator + "data" + File.separator;
    try (Stream<Path> walk = Files.walk(dir.toPath())) {
      return walk.filter(Files::isRegularFile)
          .filter(p -> p.toString().contains(dataSegment))
          .collect(Collectors.toList());
    }
  }

  /** Delegates to a real {@link FileIO}; the first file it creates fails on close after writing. */
  private static final class CloseFailingFileIO implements FileIO {
    private final FileIO delegate;
    private boolean armed = true;

    CloseFailingFileIO(FileIO delegate) {
      this.delegate = delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
      return delegate.newInputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      OutputFile file = delegate.newOutputFile(path);
      if (!armed) {
        return file;
      }
      armed = false;
      return new OutputFile() {
        @Override
        public PositionOutputStream create() {
          return failingOnClose(file.create());
        }

        @Override
        public PositionOutputStream createOrOverwrite() {
          return failingOnClose(file.createOrOverwrite());
        }

        @Override
        public String location() {
          return file.location();
        }

        @Override
        public InputFile toInputFile() {
          return file.toInputFile();
        }
      };
    }

    private static PositionOutputStream failingOnClose(PositionOutputStream out) {
      return new PositionOutputStream() {
        @Override
        public long getPos() throws IOException {
          return out.getPos();
        }

        @Override
        public void write(int b) throws IOException {
          out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
          out.flush();
        }

        @Override
        public void close() throws IOException {
          out.close();
          throw new IOException("simulated close failure");
        }
      };
    }
  }
}
