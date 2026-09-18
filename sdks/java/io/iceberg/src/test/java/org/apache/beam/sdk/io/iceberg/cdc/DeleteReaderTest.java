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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Verifies that {@link DeleteReader#read} returns the <b>union</b> of records matched by position
 * and equality deletes.
 *
 * <p>The tests stub the {@link DeleteLoader} so we exercise the predicate-composition logic
 * directly without writing real delete files. End-to-end is covered by other tests.
 */
@RunWith(JUnit4.class)
public class DeleteReaderTest {
  private static final Schema TABLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()));

  private static final DeleteFile POS_FILE =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/test/pos.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(3)
          .build();

  private static final DeleteFile EQ_FILE_ID =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofEqualityDeletes(1)
          .withPath("/test/eq.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(2)
          .build();

  private static final DeleteFile EQ_FILE_NAME =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofEqualityDeletes(2)
          .withPath("/test/eq-name.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(2)
          .build();

  /** {@link DeleteReader} that returns a stubbed {@link DeleteLoader} for tests. */
  private static class StubDeleteReader extends DeleteReader<Record> {
    private final DeleteLoader stub;

    StubDeleteReader(List<DeleteFile> deletes, DeleteLoader stub) {
      super(
          "/test/data.parquet",
          deletes,
          TABLE_SCHEMA,
          TABLE_SCHEMA,
          true,
          PreloadedDeletes.empty());
      this.stub = stub;
    }

    StubDeleteReader(
        List<DeleteFile> deletes,
        DeleteLoader stub,
        DeleteReader.PreloadedDeletes preloadedDeletes) {
      super("/test/data.parquet", deletes, TABLE_SCHEMA, TABLE_SCHEMA, true, preloadedDeletes);
      this.stub = stub;
    }

    StubDeleteReader(
        List<DeleteFile> deletes,
        DeleteLoader stub,
        Schema requestedSchema,
        boolean needRowPosCol) {
      super(
          "/test/data.parquet",
          deletes,
          TABLE_SCHEMA,
          requestedSchema,
          needRowPosCol,
          PreloadedDeletes.empty());
      this.stub = stub;
    }

    @Override
    protected StructLike asStructLike(Record record) {
      return record;
    }

    @Override
    protected InputFile getInputFile(String location) {
      throw new UnsupportedOperationException("not used with a stubbed DeleteLoader");
    }

    @Override
    protected DeleteLoader newDeleteLoader() {
      return stub;
    }
  }

  /** {@link DeleteLoader} that returns pre-built indexes. */
  private static class StubLoader implements DeleteLoader {
    private final PositionDeleteIndex posIndex;
    private final Map<Set<Integer>, StructLikeSet> eqSets;
    private int posLoadCount = 0;
    private int eqLoadCount = 0;

    StubLoader(PositionDeleteIndex posIndex, StructLikeSet eqSet) {
      this(posIndex, Collections.singletonMap(Collections.singleton(1), eqSet));
    }

    StubLoader(PositionDeleteIndex posIndex, Map<Set<Integer>, StructLikeSet> eqSets) {
      this.posIndex = posIndex;
      this.eqSets = eqSets;
    }

    @Override
    public PositionDeleteIndex loadPositionDeletes(Iterable<DeleteFile> files, CharSequence path) {
      posLoadCount++;
      return posIndex;
    }

    @Override
    public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> files, Schema schema) {
      eqLoadCount++;
      return eqSets.getOrDefault(
          Sets.newHashSet(TypeUtil.getProjectedIds(new Schema(schema.asStruct().fields()))),
          StructLikeSet.create(schema.asStruct()));
    }
  }

  /** A minimal HashSet-backed {@link PositionDeleteIndex} for tests. */
  private static PositionDeleteIndex posIndexOf(long... positions) {
    Set<Long> backing = new HashSet<>();
    for (long p : positions) {
      backing.add(p);
    }
    return new PositionDeleteIndex() {
      @Override
      public void delete(long pos) {
        backing.add(pos);
      }

      @Override
      public void delete(long from, long to) {
        for (long p = from; p < to; p++) {
          backing.add(p);
        }
      }

      @Override
      public boolean isDeleted(long pos) {
        return backing.contains(pos);
      }

      @Override
      public boolean isEmpty() {
        return backing.isEmpty();
      }

      @Override
      public long cardinality() {
        return backing.size();
      }
    };
  }

  private static StructLikeSet eqSetOfIds(int... ids) {
    Schema idSchema = TABLE_SCHEMA.select("id");
    StructLikeSet set = StructLikeSet.create(idSchema.asStruct());
    for (int id : ids) {
      GenericRecord r = GenericRecord.create(idSchema);
      r.setField("id", id);
      set.add(r);
    }
    return set;
  }

  private static StructLikeSet eqSetOfNames(String... names) {
    Schema nameSchema = TABLE_SCHEMA.select("name");
    StructLikeSet set = StructLikeSet.create(nameSchema.asStruct());
    for (String name : names) {
      GenericRecord r = GenericRecord.create(nameSchema);
      r.setField("name", name);
      set.add(r);
    }
    return set;
  }

  /** Builds N records (id=0..N-1, name="v0".."vN-1") matching {@code readSchema}. */
  private static List<Record> records(Schema readSchema, int n) {
    boolean hasPos = readSchema.findField("_pos") != null;
    List<Record> recs = new ArrayList<>(n);
    for (long i = 0; i < n; i++) {
      GenericRecord r = GenericRecord.create(readSchema);
      r.setField("id", (int) i);
      r.setField("name", "v" + i);
      if (hasPos) {
        r.setField("_pos", i);
      }
      recs.add(r);
    }
    return recs;
  }

  /** Sorted list of "id" values from the output, for stable assertions. */
  private static List<Integer> idsOf(CloseableIterable<Record> records) {
    return ImmutableList.copyOf(records).stream()
        .map(r -> (Integer) r.getField("id"))
        .sorted()
        .collect(Collectors.toList());
  }

  /** With no delete files at all, {@code read()} emits nothing. */
  @Test
  public void noDeletesEmitsNothing() {
    DeleteLoader loader = new StubLoader(posIndexOf(), eqSetOfIds());
    DeleteReader<Record> reader = new StubDeleteReader(Collections.emptyList(), loader);
    List<Record> input = records(reader.requiredSchema(), 5);

    CloseableIterable<Record> output = reader.read(CloseableIterable.withNoopClose(input));

    assertEquals(Collections.emptyList(), idsOf(output));
  }

  /** Pos-only emits only the pos-deleted records. */
  @Test
  public void posOnlyEmitsPosDeletedRecords() {
    DeleteLoader loader = new StubLoader(posIndexOf(1L, 3L), eqSetOfIds());
    DeleteReader<Record> reader = new StubDeleteReader(ImmutableList.of(POS_FILE), loader);
    List<Record> input = records(reader.requiredSchema(), 5);

    CloseableIterable<Record> output = reader.read(CloseableIterable.withNoopClose(input));

    assertEquals(ImmutableList.of(1, 3), idsOf(output));
  }

  /** Only equality deletes, emits records matching the eq set. */
  @Test
  public void eqOnlyEmitsEqDeletedRecords() {
    DeleteLoader loader = new StubLoader(posIndexOf(), eqSetOfIds(2, 4));
    DeleteReader<Record> reader = new StubDeleteReader(ImmutableList.of(EQ_FILE_ID), loader);
    List<Record> input = records(reader.requiredSchema(), 5);

    CloseableIterable<Record> output = reader.read(CloseableIterable.withNoopClose(input));

    assertEquals(ImmutableList.of(2, 4), idsOf(output));
  }

  /** Pos-deletes plus equality deletes, emit the union without duplication. */
  @Test
  public void posAndEqEmitUnion() {
    DeleteLoader loader = new StubLoader(posIndexOf(0L, 4L), eqSetOfIds(2, 4));
    DeleteReader<Record> reader =
        new StubDeleteReader(ImmutableList.of(POS_FILE, EQ_FILE_ID), loader);
    List<Record> input = records(reader.requiredSchema(), 6);

    CloseableIterable<Record> output = reader.read(CloseableIterable.withNoopClose(input));

    // id 4 is in both sides; it must appear exactly once.
    assertEquals(ImmutableList.of(0, 2, 4), idsOf(output));
  }

  /** Preloaded position deletes are reused instead of loading the same delete files again. */
  @Test
  public void preloadedPositionDeletesAvoidSecondLoad() {
    StubLoader loader = new StubLoader(posIndexOf(), eqSetOfIds());
    PositionDeleteIndex preloadedPosIndex = posIndexOf(1L, 3L);
    DeleteReader<Record> reader =
        new StubDeleteReader(
            ImmutableList.of(POS_FILE),
            loader,
            DeleteReader.PreloadedDeletes.of(preloadedPosIndex, Collections.emptyMap()));
    List<Record> input = records(reader.requiredSchema(), 5);

    CloseableIterable<Record> output = reader.read(CloseableIterable.withNoopClose(input));

    assertEquals(ImmutableList.of(1, 3), idsOf(output));
    assertEquals(0, loader.posLoadCount);
  }

  /** Preloaded equality delete sets are reused instead of loading the same delete files again. */
  @Test
  public void preloadedEqualityDeletesAvoidSecondLoad() {
    StubLoader loader = new StubLoader(posIndexOf(), eqSetOfIds());
    Map<Set<Integer>, StructLikeSet> preloadedEqSets = new HashMap<>();
    preloadedEqSets.put(Collections.singleton(1), eqSetOfIds(2, 4));
    DeleteReader<Record> reader =
        new StubDeleteReader(
            ImmutableList.of(EQ_FILE_ID),
            loader,
            DeleteReader.PreloadedDeletes.of(null, preloadedEqSets));
    List<Record> input = records(reader.requiredSchema(), 5);

    CloseableIterable<Record> output = reader.read(CloseableIterable.withNoopClose(input));

    assertEquals(ImmutableList.of(2, 4), idsOf(output));
    assertEquals(0, loader.eqLoadCount);
  }

  @Test
  public void requiredSchemaAddsUnprojectedEqualityDeleteField() {
    Schema requestedSchema = TABLE_SCHEMA.select("id");
    DeleteLoader loader =
        new StubLoader(
            posIndexOf(), Collections.singletonMap(Collections.singleton(2), eqSetOfNames("v2")));
    DeleteReader<Record> reader =
        new StubDeleteReader(ImmutableList.of(EQ_FILE_NAME), loader, requestedSchema, true);

    assertEquals(
        ImmutableList.of("id", "name"),
        reader.requiredSchema().columns().stream()
            .map(Types.NestedField::name)
            .collect(Collectors.toList()));

    List<Record> input = records(reader.requiredSchema(), 4);
    CloseableIterable<Record> output = reader.read(CloseableIterable.withNoopClose(input));

    assertEquals(ImmutableList.of(2), idsOf(output));
  }

  @Test
  public void rowPositionColumnIsOnlyAddedWhenRequiredForPositionDeletes() {
    DeleteLoader loader = new StubLoader(posIndexOf(1L), eqSetOfIds());

    DeleteReader<Record> posReaderNeedsPos =
        new StubDeleteReader(ImmutableList.of(POS_FILE), loader, TABLE_SCHEMA, true);
    DeleteReader<Record> posReaderDoesNotNeedPos =
        new StubDeleteReader(ImmutableList.of(POS_FILE), loader, TABLE_SCHEMA, false);
    DeleteReader<Record> eqReader =
        new StubDeleteReader(ImmutableList.of(EQ_FILE_ID), loader, TABLE_SCHEMA, true);

    assertNotNull(posReaderNeedsPos.requiredSchema().findField("_pos"));
    assertNull(posReaderDoesNotNeedPos.requiredSchema().findField("_pos"));
    assertNull(eqReader.requiredSchema().findField("_pos"));
  }

  @Test
  public void multipleEqualityDeleteGroupsAreOrCombined() {
    Map<Set<Integer>, StructLikeSet> eqSets = new HashMap<>();
    eqSets.put(Collections.singleton(1), eqSetOfIds(1));
    eqSets.put(Collections.singleton(2), eqSetOfNames("v3"));
    StubLoader loader = new StubLoader(posIndexOf(), eqSets);
    DeleteReader<Record> reader =
        new StubDeleteReader(ImmutableList.of(EQ_FILE_ID, EQ_FILE_NAME), loader);

    CloseableIterable<Record> output =
        reader.read(CloseableIterable.withNoopClose(records(reader.requiredSchema(), 5)));

    assertEquals(ImmutableList.of(1, 3), idsOf(output));
    assertEquals(2, loader.eqLoadCount);
  }

  @Test
  public void preloadedEqualityDeleteKeysAreDefensivelyCopied() {
    StructLikeSet idDeletes = eqSetOfIds(2);
    Set<Integer> mutableKey = new HashSet<>(Collections.singleton(1));
    Map<Set<Integer>, StructLikeSet> preloadedEqSets = new HashMap<>();
    preloadedEqSets.put(mutableKey, idDeletes);

    DeleteReader.PreloadedDeletes preloadedDeletes =
        DeleteReader.PreloadedDeletes.of(null, preloadedEqSets);
    mutableKey.add(2);

    assertEquals(idDeletes, preloadedDeletes.equalityDeleteSet(Collections.singleton(1)));

    StubLoader loader = new StubLoader(posIndexOf(), eqSetOfIds());
    DeleteReader<Record> reader =
        new StubDeleteReader(ImmutableList.of(EQ_FILE_ID), loader, preloadedDeletes);
    CloseableIterable<Record> output =
        reader.read(CloseableIterable.withNoopClose(records(reader.requiredSchema(), 4)));

    assertEquals(ImmutableList.of(2), idsOf(output));
    assertEquals(0, loader.eqLoadCount);
  }
}
