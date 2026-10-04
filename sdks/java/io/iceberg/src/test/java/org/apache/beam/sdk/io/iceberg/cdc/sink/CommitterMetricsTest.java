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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import org.apache.beam.sdk.io.iceberg.cdc.sink.CommitterMetrics.CommitSummary;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CommitterMetrics}. */
@RunWith(JUnit4.class)
public class CommitterMetricsTest {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final PartitionSpec SPEC_0 = PartitionSpec.unpartitioned();
  private static final PartitionSpec SPEC_1 =
      PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();

  private static DataFile dataFile(PartitionSpec spec, long records, long bytes) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("/data/" + records + "-" + bytes + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(records)
            .withFileSizeInBytes(bytes);
    return (spec.isPartitioned() ? builder.withPartitionPath("id=1") : builder).build();
  }

  private static DeleteFile deleteFile(
      PartitionSpec spec, boolean equality, long records, long bytes) {
    FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec);
    builder = equality ? builder.ofEqualityDeletes(1) : builder.ofPositionDeletes();
    return builder
        .withPath("/deletes/" + records + "-" + bytes + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(records)
        .withFileSizeInBytes(bytes)
        .build();
  }

  // Counts, records, and bytes cover both file kinds; only equality deletes count as delete
  // records.
  @Test
  public void summarizesCountsRecordsAndBytes() {
    CommitSummary summary =
        CommitSummary.of(
            ImmutableList.of(dataFile(SPEC_0, 10, 100), dataFile(SPEC_0, 20, 200)),
            ImmutableList.of(deleteFile(SPEC_0, true, 4, 40), deleteFile(SPEC_0, false, 7, 70)));

    assertThat(summary.dataFileCount, equalTo(2L));
    assertThat(summary.deleteFileCount, equalTo(2L));
    assertThat(summary.dataRecords, equalTo(30L));
    assertThat(summary.equalityDeleteRecords, equalTo(4L));
    assertThat(summary.bytes, equalTo(410L));
    assertThat(summary.hasEqualityDeletes, equalTo(true));
  }

  // A window with only position deletes carries no equality deletes.
  @Test
  public void positionDeletesAreNotEqualityDeletes() {
    CommitSummary summary =
        CommitSummary.of(ImmutableList.of(), ImmutableList.of(deleteFile(SPEC_0, false, 7, 70)));

    assertThat(summary.equalityDeleteRecords, equalTo(0L));
    assertThat(summary.hasEqualityDeletes, equalTo(false));
    assertThat(summary.bytes, equalTo(70L));
  }

  // The first spec id comes from the data files when there are any, and every spec id is collected.
  @Test
  public void firstSpecIdPrefersDataFilesAndSpecIdsCollectsAll() {
    CommitSummary mixed =
        CommitSummary.of(
            ImmutableList.of(dataFile(SPEC_1, 1, 1)),
            ImmutableList.of(deleteFile(SPEC_0, true, 1, 1)));
    assertThat(mixed.firstSpecId, equalTo(1));
    assertThat(mixed.specIds, containsInAnyOrder(0, 1));

    CommitSummary deletesOnly =
        CommitSummary.of(ImmutableList.of(), ImmutableList.of(deleteFile(SPEC_0, true, 1, 1)));
    assertThat(deletesOnly.firstSpecId, equalTo(0));
  }

  @Test
  public void emptyWindowHasNoSpec() {
    CommitSummary summary = CommitSummary.of(ImmutableList.of(), ImmutableList.of());

    assertThat(summary.dataFileCount, equalTo(0L));
    assertThat(summary.bytes, equalTo(0L));
    assertThat(summary.firstSpecId, nullValue());
    assertThat(summary.specIds, empty());
  }
}
