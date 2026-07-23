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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.OutputBuilder;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ChangelogScanner}. */
@RunWith(JUnit4.class)
public class ChangelogScannerTest {
  private static final Schema SINGLE_PK_SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));
  private static final Schema COMPOSITE_PK_SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "account", Types.StringType.get()),
              required(2, "sequence", Types.IntegerType.get()),
              optional(3, "data", Types.StringType.get())),
          ImmutableSet.of(1, 2));
  private static final Schema SINGLE_RECORD_ID_SCHEMA = recordIdSchema(SINGLE_PK_SCHEMA);
  private static final Schema COMPOSITE_RECORD_ID_SCHEMA = recordIdSchema(COMPOSITE_PK_SCHEMA);
  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.unpartitioned();

  @Test
  public void analyzeFilesPrunesNonOverlappingOpposingTasksToUnidirectional() {
    FakeAddedRowsTask insert = new FakeAddedRowsTask(dataFile("insert", 10L, 20L), 11L);
    FakeDeletedDataFileTask delete = new FakeDeletedDataFileTask(dataFile("delete", 30L, 40L), 13L);

    ChangelogScanner.AnalysisResult result =
        ChangelogScanner.analyzeFiles(
            true,
            ImmutableList.of(insert, delete),
            SINGLE_RECORD_ID_SCHEMA,
            comparator(SINGLE_RECORD_ID_SCHEMA));

    assertThat(result.bidirectional, empty());
    assertThat(result.unidirectional, contains(delete, insert));
    assertNull(result.overlapLower);
    assertNull(result.overlapUpper);
  }

  @Test
  public void analyzeFilesFindsOverlapDespiteInputOrder() {
    FakeDeletedDataFileTask laterDelete =
        new FakeDeletedDataFileTask(dataFile("delete-later", 30L, 40L), 13L);
    FakeDeletedDataFileTask overlappingDelete =
        new FakeDeletedDataFileTask(dataFile("delete-overlap", 15L, 18L), 17L);
    FakeAddedRowsTask insert = new FakeAddedRowsTask(dataFile("insert", 10L, 20L), 19L);

    ChangelogScanner.AnalysisResult result =
        ChangelogScanner.analyzeFiles(
            true,
            ImmutableList.of(laterDelete, overlappingDelete, insert),
            SINGLE_RECORD_ID_SCHEMA,
            comparator(SINGLE_RECORD_ID_SCHEMA));

    assertThat(result.unidirectional, contains(laterDelete));
    assertThat(result.bidirectional, containsInAnyOrder(overlappingDelete, insert));
    assertEquals(15L, record(result.overlapLower).getField("id"));
    assertEquals(18L, record(result.overlapUpper).getField("id"));
  }

  @Test
  public void analyzeFilesUsesLexicographicCompositePrimaryKeyRanges() {
    FakeAddedRowsTask insert =
        new FakeAddedRowsTask(
            dataFile(
                COMPOSITE_PK_SCHEMA,
                "insert",
                ImmutableMap.of("account", "acct-a", "sequence", 2),
                ImmutableMap.of("account", "acct-a", "sequence", 8),
                11L),
            11L);
    FakeDeletedDataFileTask overlappingDelete =
        new FakeDeletedDataFileTask(
            dataFile(
                COMPOSITE_PK_SCHEMA,
                "delete-overlap",
                ImmutableMap.of("account", "acct-a", "sequence", 5),
                ImmutableMap.of("account", "acct-b", "sequence", 1),
                13L),
            13L);
    FakeDeletedDataFileTask farDelete =
        new FakeDeletedDataFileTask(
            dataFile(
                COMPOSITE_PK_SCHEMA,
                "delete-far",
                ImmutableMap.of("account", "acct-c", "sequence", 1),
                ImmutableMap.of("account", "acct-c", "sequence", 2),
                17L),
            17L);

    ChangelogScanner.AnalysisResult result =
        ChangelogScanner.analyzeFiles(
            true,
            ImmutableList.of(insert, farDelete, overlappingDelete),
            COMPOSITE_RECORD_ID_SCHEMA,
            comparator(COMPOSITE_RECORD_ID_SCHEMA));

    assertThat(result.unidirectional, contains(farDelete));
    assertThat(result.bidirectional, containsInAnyOrder(insert, overlappingDelete));
    assertEquals("acct-a", record(result.overlapLower).getField("account").toString());
    assertEquals(5, record(result.overlapLower).getField("sequence"));
    assertEquals("acct-a", record(result.overlapUpper).getField("account").toString());
    assertEquals(8, record(result.overlapUpper).getField("sequence"));
  }

  @Test
  public void analyzeFilesConservativelyRoutesAllTasksWhenMetricsAreMissing() {
    FakeAddedRowsTask insert = new FakeAddedRowsTask(dataFile("insert", 10L, 20L), 11L);
    FakeDeletedDataFileTask deleteWithMissingMetrics =
        new FakeDeletedDataFileTask(dataFileWithoutBounds("delete-missing-metrics"), 13L);

    ChangelogScanner.AnalysisResult result =
        ChangelogScanner.analyzeFiles(
            true, // should catch missing metrics even if table claims it tracks them
            ImmutableList.of(insert, deleteWithMissingMetrics),
            SINGLE_RECORD_ID_SCHEMA,
            comparator(SINGLE_RECORD_ID_SCHEMA));

    assertThat(result.unidirectional, empty());
    assertThat(result.bidirectional, contains(insert, deleteWithMissingMetrics));
    assertNull(result.overlapLower);
    assertNull(result.overlapUpper);
  }

  @Test
  public void taskBatcherFlushesAtSplitBoundariesWithoutEmptyBatches() {
    CapturingOutputReceiver out = new CapturingOutputReceiver();
    ChangelogScanner.TaskBatcher batcher =
        new ChangelogScanner.TaskBatcher("default.table", 1234L, 100L, out);
    SerializableChangelogTask first = serializableTask("first", 40L);
    SerializableChangelogTask second = serializableTask("second", 60L);
    SerializableChangelogTask third = serializableTask("third", 1L);

    batcher.add(first, 1L, 40L);
    batcher.add(second, 1L, 60L);
    assertThat(out.values, empty());

    batcher.add(third, 1L, 1L);
    assertEquals(1, out.values.size());
    batcher.flush();

    assertEquals(2, out.values.size());
    assertEquals(2, batcher.totalSplits);
    assertEquals(new Instant(1234L), out.values.get(0).getTimestamp());
    assertThat(tasksInOutput(out, 0), contains(first, second));
    assertThat(tasksInOutput(out, 1), contains(third));
  }

  @Test
  public void taskBatcherAllowsOversizeSingleTaskWithoutEmittingEmptyBatch() {
    CapturingOutputReceiver out = new CapturingOutputReceiver();
    ChangelogScanner.TaskBatcher batcher =
        new ChangelogScanner.TaskBatcher("default.table", 1234L, 100L, out);
    SerializableChangelogTask oversize = serializableTask("oversize", 150L);

    batcher.add(oversize, 1L, 150L);
    assertThat(out.values, empty());

    batcher.flush();

    assertEquals(1, out.values.size());
    assertEquals(1, batcher.totalSplits);
    assertThat(tasksInOutput(out, 0), contains(oversize));
  }

  private static Comparator<StructLike> comparator(Schema schema) {
    return Comparators.forType(schema.asStruct());
  }

  private static Schema recordIdSchema(Schema schema) {
    return TypeUtil.select(schema, schema.identifierFieldIds());
  }

  private static Record record(StructLike structLike) {
    return (Record) structLike;
  }

  private static List<SerializableChangelogTask> tasksInOutput(
      CapturingOutputReceiver out, int index) {
    return out.values.get(index).getValue().getValue();
  }

  private static DataFile dataFile(String name, long lower, long upper) {
    return dataFile(UNPARTITIONED_SPEC, null, name, lower, upper);
  }

  private static DataFile dataFile(
      PartitionSpec spec, StructLike partition, String name, long lower, long upper) {
    return dataFile(
        SINGLE_PK_SCHEMA,
        spec,
        partition,
        name,
        ImmutableMap.of("id", lower),
        ImmutableMap.of("id", upper),
        100L);
  }

  private static DataFile dataFile(
      Schema schema, String name, Map<String, Object> lower, Map<String, Object> upper, long size) {
    return dataFile(schema, UNPARTITIONED_SPEC, null, name, lower, upper, size);
  }

  private static DataFile dataFile(
      Schema schema,
      PartitionSpec spec,
      StructLike partition,
      String name,
      Map<String, Object> lower,
      Map<String, Object> upper,
      long size) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withFormat(FileFormat.PARQUET)
            .withPath("gs:://bucket/data/" + name + ".parquet")
            .withFileSizeInBytes(size)
            .withMetrics(
                new Metrics(
                    1L, null, null, null, null, bounds(schema, lower), bounds(schema, upper)));
    if (partition != null) {
      builder.withPartition(partition);
    }
    return builder.build();
  }

  private static DataFile dataFileWithoutBounds(String name) {
    return DataFiles.builder(UNPARTITIONED_SPEC)
        .withFormat(FileFormat.PARQUET)
        .withPath("gs:://bucket/data/" + name + ".parquet")
        .withFileSizeInBytes(100L)
        .withMetrics(new Metrics(1L, null, null, null, null, null, null))
        .build();
  }

  private static Map<Integer, ByteBuffer> bounds(Schema schema, Map<String, Object> values) {
    ImmutableMap.Builder<Integer, ByteBuffer> builder = ImmutableMap.builder();
    for (Types.NestedField field : schema.columns()) {
      if (values.containsKey(field.name())) {
        builder.put(
            field.fieldId(), Conversions.toByteBuffer(field.type(), values.get(field.name())));
      }
    }
    return builder.build();
  }

  private static SerializableChangelogTask serializableTask(String name, long length) {
    DataFile file =
        DataFiles.builder(UNPARTITIONED_SPEC)
            .withFormat(FileFormat.PARQUET)
            .withPath("gs:://bucket/data/" + name + ".parquet")
            .withFileSizeInBytes(length)
            .withMetrics(new Metrics(1L, null, null, null, null, null, null))
            .build();
    return SerializableChangelogTask.builder()
        .setType(SerializableChangelogTask.Type.ADDED_ROWS)
        .setDataFile(SerializableDataFile.from(file, "", false))
        .setSpecId(UNPARTITIONED_SPEC.specId())
        .setOperation(ChangelogOperation.INSERT)
        .setOrdinal(0)
        .setCommitSnapshotId(1L)
        .setStart(0L)
        .setLength(length)
        .setJsonExpression(ExpressionParser.toJson(Expressions.alwaysTrue()))
        .build();
  }

  private abstract static class FakeContentTask
      implements ChangelogScanTask, ContentScanTask<DataFile> {
    private final DataFile file;
    private final PartitionSpec spec;
    private final StructLike partition;
    private final long length;

    FakeContentTask(DataFile file, long length) {
      this(file, UNPARTITIONED_SPEC, file.partition(), length);
    }

    FakeContentTask(DataFile file, PartitionSpec spec, StructLike partition, long length) {
      this.file = file;
      this.spec = spec;
      this.partition = partition;
      this.length = length;
    }

    @Override
    public DataFile file() {
      return file;
    }

    @Override
    public PartitionSpec spec() {
      return spec;
    }

    @Override
    public StructLike partition() {
      return partition;
    }

    @Override
    public long start() {
      return 0L;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public Expression residual() {
      return Expressions.alwaysTrue();
    }

    @Override
    public int changeOrdinal() {
      return 0;
    }

    @Override
    public long commitSnapshotId() {
      return 1L;
    }
  }

  private static class FakeAddedRowsTask extends FakeContentTask implements AddedRowsScanTask {
    FakeAddedRowsTask(DataFile file, long length) {
      super(file, length);
    }

    @Override
    public List<DeleteFile> deletes() {
      return Collections.emptyList();
    }

    @Override
    public ChangelogOperation operation() {
      return ChangelogOperation.INSERT;
    }
  }

  private static class FakeDeletedDataFileTask extends FakeContentTask
      implements DeletedDataFileScanTask {
    FakeDeletedDataFileTask(DataFile file, long length) {
      super(file, length);
    }

    @Override
    public List<DeleteFile> existingDeletes() {
      return Collections.emptyList();
    }

    @Override
    public ChangelogOperation operation() {
      return ChangelogOperation.DELETE;
    }
  }

  private static final class CapturingOutputReceiver
      implements DoFn.OutputReceiver<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> {
    private final ImmutableList.Builder<
            TimestampedValue<KV<ChangelogDescriptor, List<SerializableChangelogTask>>>>
        builder = ImmutableList.builder();
    private List<TimestampedValue<KV<ChangelogDescriptor, List<SerializableChangelogTask>>>>
        values = ImmutableList.of();

    @Override
    public OutputBuilder<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> builder(
        KV<ChangelogDescriptor, List<SerializableChangelogTask>> value) {
      throw new UnsupportedOperationException("Use outputWithTimestamp in this test receiver.");
    }

    @Override
    public void outputWithTimestamp(
        KV<ChangelogDescriptor, List<SerializableChangelogTask>> value, Instant timestamp) {
      builder.add(TimestampedValue.of(value, timestamp));
      values = builder.build();
    }
  }
}
