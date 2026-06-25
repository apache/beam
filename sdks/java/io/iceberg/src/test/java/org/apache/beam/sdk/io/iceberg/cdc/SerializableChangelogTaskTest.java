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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SerializableChangelogTask}. */
@RunWith(JUnit4.class)
public class SerializableChangelogTaskTest {
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final DataFile DATA_FILE =
      DataFiles.builder(SPEC)
          .withFormat(FileFormat.PARQUET)
          .withPath("gs://bucket/data/file.parquet")
          .withFileSizeInBytes(512L)
          .withMetrics(new Metrics(3L, null, null, null, null, null, null))
          .build();
  private static final DeleteFile ADDED_DELETE =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("gs://bucket/delete/added.parquet")
          .withFileSizeInBytes(32L)
          .withRecordCount(1L)
          .build();
  private static final DeleteFile EXISTING_DELETE =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("gs://bucket/delete/existing.parquet")
          .withFileSizeInBytes(64L)
          .withRecordCount(2L)
          .build();

  @Test
  public void coderRoundTripPreservesTaskBasics() throws Exception {
    SerializableChangelogTask task =
        SerializableChangelogTask.builder()
            .setType(SerializableChangelogTask.Type.ADDED_ROWS)
            .setDataFile(SerializableDataFile.from(DATA_FILE, "", false))
            .setSpecId(SPEC.specId())
            .setOperation(ChangelogOperation.INSERT)
            .setOrdinal(7)
            .setCommitSnapshotId(123L)
            .setStart(5L)
            .setLength(99L)
            .setJsonExpression(ExpressionParser.toJson(Expressions.alwaysTrue()))
            .build();

    SerializableChangelogTask decoded = CoderUtils.clone(SerializableChangelogTask.coder(), task);

    assertEquals(SerializableChangelogTask.Type.ADDED_ROWS, decoded.getType());
    assertEquals(ChangelogOperation.INSERT, decoded.getOperation());
    assertEquals(7, decoded.getOrdinal());
    assertEquals(123L, decoded.getCommitSnapshotId());
    assertEquals(5L, decoded.getStart());
    assertEquals(99L, decoded.getLength());
    assertEquals(DATA_FILE.location(), decoded.getDataFile().getPath());
    assertEquals(DATA_FILE.fileSizeInBytes(), decoded.getDataFile().getFileSizeInBytes());
    assertEquals(Collections.emptyList(), decoded.getExistingDeletes());
    assertEquals(Collections.emptyList(), decoded.getAddedDeletes());
    assertEquals(Expressions.alwaysTrue().toString(), decoded.getExpression(null).toString());
  }

  @Test
  public void helperMethodsReadSupportedTaskTypes() {
    FakeAddedRowsTask added = new FakeAddedRowsTask(ImmutableList.of(ADDED_DELETE));
    FakeDeletedRowsTask deletedRows =
        new FakeDeletedRowsTask(ImmutableList.of(ADDED_DELETE), ImmutableList.of(EXISTING_DELETE));
    FakeDeletedDataFileTask deletedFile =
        new FakeDeletedDataFileTask(ImmutableList.of(EXISTING_DELETE));

    assertEquals(
        SerializableChangelogTask.Type.ADDED_ROWS, SerializableChangelogTask.getType(added));
    assertEquals(
        SerializableChangelogTask.Type.DELETED_ROWS,
        SerializableChangelogTask.getType(deletedRows));
    assertEquals(
        SerializableChangelogTask.Type.DELETED_FILE,
        SerializableChangelogTask.getType(deletedFile));

    assertEquals(22L, SerializableChangelogTask.getLength(added));
    assertEquals(
        44L, SerializableChangelogTask.getTotalLength(ImmutableList.of(added, deletedRows)));
    assertSame(DATA_FILE, SerializableChangelogTask.getDataFile(deletedRows));
    assertSame(SPEC, SerializableChangelogTask.getSpec(deletedFile));
    assertSame(DATA_FILE.partition(), SerializableChangelogTask.getPartition(added));
    assertThat(SerializableChangelogTask.getAddedDeleteFiles(added), contains(ADDED_DELETE));
    assertThat(SerializableChangelogTask.getAddedDeleteFiles(deletedRows), contains(ADDED_DELETE));
    assertEquals(
        Collections.emptyList(), SerializableChangelogTask.getAddedDeleteFiles(deletedFile));
  }

  @Test
  public void unsupportedTaskTypeFailsClearly() {
    ChangelogScanTask unsupported =
        new ChangelogScanTask() {
          @Override
          public ChangelogOperation operation() {
            return ChangelogOperation.INSERT;
          }

          @Override
          public int changeOrdinal() {
            return 0;
          }

          @Override
          public long commitSnapshotId() {
            return 0L;
          }
        };

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> SerializableChangelogTask.getLength(unsupported));

    assertThat(
        thrown.getMessage(),
        containsString("Unknown ChangelogScanTask type: " + unsupported.getClass()));
  }

  private abstract static class FakeContentTask
      implements ChangelogScanTask, ContentScanTask<DataFile> {
    @Override
    public DataFile file() {
      return DATA_FILE;
    }

    @Override
    public PartitionSpec spec() {
      return SPEC;
    }

    @Override
    public StructLike partition() {
      return DATA_FILE.partition();
    }

    @Override
    public long start() {
      return 11L;
    }

    @Override
    public long length() {
      return 22L;
    }

    @Override
    public Expression residual() {
      return Expressions.alwaysTrue();
    }

    @Override
    public int changeOrdinal() {
      return 2;
    }

    @Override
    public long commitSnapshotId() {
      return 101L;
    }
  }

  private static class FakeAddedRowsTask extends FakeContentTask implements AddedRowsScanTask {
    private final List<DeleteFile> deletes;

    FakeAddedRowsTask(List<DeleteFile> deletes) {
      this.deletes = deletes;
    }

    @Override
    public List<DeleteFile> deletes() {
      return deletes;
    }
  }

  private static class FakeDeletedRowsTask extends FakeContentTask implements DeletedRowsScanTask {
    private final List<DeleteFile> addedDeletes;
    private final List<DeleteFile> existingDeletes;

    FakeDeletedRowsTask(List<DeleteFile> addedDeletes, List<DeleteFile> existingDeletes) {
      this.addedDeletes = addedDeletes;
      this.existingDeletes = existingDeletes;
    }

    @Override
    public List<DeleteFile> addedDeletes() {
      return addedDeletes;
    }

    @Override
    public List<DeleteFile> existingDeletes() {
      return existingDeletes;
    }
  }

  private static class FakeDeletedDataFileTask extends FakeContentTask
      implements DeletedDataFileScanTask {
    private final List<DeleteFile> existingDeletes;

    FakeDeletedDataFileTask(List<DeleteFile> existingDeletes) {
      this.existingDeletes = existingDeletes;
    }

    @Override
    public List<DeleteFile> existingDeletes() {
      return existingDeletes;
    }
  }
}
