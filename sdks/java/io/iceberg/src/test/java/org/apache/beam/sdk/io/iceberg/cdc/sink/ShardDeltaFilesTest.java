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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ShardDeltaFiles}, the element the write stage hands the committer: schema-coder
 * round-trips (including the empty-list cases), and — the invariant that made direct transport
 * viable at all — that a partition value survives the coder and reconstructs into an identical
 * Iceberg partition tuple, including values a rendered partition <i>path</i> could not round-trip.
 *
 * <p>There is deliberately no wire-version case: unlike its staged-manifest predecessor, whose
 * payload was an opaque Iceberg-format blob, this type is schema-coded over its own fields and
 * evolves by adding fields. See the class Javadoc. That argument is only sound if the field numbers
 * themselves are stable, which is what {@link #schemaFieldNumbersArePinned} and {@link
 * #windowedCommitSchemaFieldNumbersArePinned} exist to guarantee.
 */
@RunWith(JUnit4.class)
public class ShardDeltaFilesTest {

  private static final Schema ICEBERG_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()));

  /** Identity-partitioned on the STRING column, so partition values can contain a {@code '/'}. */
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("name").build();

  private static final Map<Integer, PartitionSpec> SPECS = ImmutableMap.of(SPEC.specId(), SPEC);

  private static Coder<ShardDeltaFiles> coder() {
    return WriteDeltas.shardDeltaFilesCoder();
  }

  /** The partition tuple {@code SPEC} produces for a row whose {@code name} is {@code value}. */
  private static PartitionKey partitionFor(String value) {
    GenericRecord record = GenericRecord.create(ICEBERG_SCHEMA);
    record.setField("id", 1);
    record.setField("name", value);
    PartitionKey key = new PartitionKey(SPEC, ICEBERG_SCHEMA);
    key.partition(record);
    return key;
  }

  private static DataFile dataFile(String path, String partitionValue) {
    return DataFiles.builder(SPEC)
        .withFormat(FileFormat.PARQUET)
        .withPath(path)
        .withPartition(partitionFor(partitionValue))
        .withFileSizeInBytes(100L)
        .withRecordCount(2L)
        .build();
  }

  /** A V3 deletion vector: PUFFIN position deletes carrying the three DV-only fields. */
  private static DeleteFile deletionVector(String path, String referenced, String partitionValue) {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath(path)
        .withFormat(FileFormat.PUFFIN)
        .withPartition(partitionFor(partitionValue))
        .withFileSizeInBytes(80L)
        .withRecordCount(1L)
        .withContentOffset(4L)
        .withContentSizeInBytes(40L)
        .withReferencedDataFile(referenced)
        .build();
  }

  private static ShardDeltaFiles shard(
      Iterable<SerializableDataFile> dataFiles, Iterable<SerializableDeleteFile> deleteFiles) {
    return ShardDeltaFiles.builder()
        .setTableIdentifierString("db.t")
        .setDataFiles(ImmutableList.copyOf(dataFiles))
        .setDeleteFiles(ImmutableList.copyOf(deleteFiles))
        .setMinSequenceNumber(1L)
        .setMaxSequenceNumber(2L)
        .build();
  }

  @Test
  public void coderRoundTripsAllFieldsAndEmptyLists() throws Exception {
    // facet: every field populated.
    ShardDeltaFiles files =
        shard(
            ImmutableList.of(
                SerializableDataFile.from(dataFile("/tmp/d-1.parquet", "a"), SPEC),
                SerializableDataFile.from(dataFile("/tmp/d-2.parquet", "b"), SPEC)),
            ImmutableList.of(
                SerializableDeleteFile.from(
                    deletionVector("/tmp/dv-1.puffin", "/tmp/d-1.parquet", "a"), SPEC)));

    Coder<ShardDeltaFiles> coder = coder();
    CoderProperties.coderDecodeEncodeEqual(coder, files);

    ShardDeltaFiles decoded =
        CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, files));
    assertThat(decoded.getTableIdentifierString(), equalTo("db.t"));
    assertThat(decoded.getMinSequenceNumber(), equalTo(1L));
    assertThat(decoded.getMaxSequenceNumber(), equalTo(2L));
    assertThat(decoded.getDataFiles(), hasSize(2));
    assertThat(decoded.getDeleteFiles(), hasSize(1));
    assertThat(decoded.getDataFiles().get(0).getPath(), equalTo("/tmp/d-1.parquet"));
    assertThat(decoded.getDeleteFiles().get(0).getLocation(), equalTo("/tmp/dv-1.puffin"));

    // facet: both lists empty — a shard may carry only data files or only delete files.
    ShardDeltaFiles emptyLists = shard(ImmutableList.of(), ImmutableList.of());
    CoderProperties.coderDecodeEncodeEqual(coder, emptyLists);
    ShardDeltaFiles decodedEmpty =
        CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, emptyLists));
    assertThat(decodedEmpty.getDataFiles(), empty());
    assertThat(decodedEmpty.getDeleteFiles(), empty());
  }

  /**
   * The load-bearing property of this transport: a partition value survives the coder and rebuilds
   * into the same Iceberg partition tuple. The value carries a {@code '/'} on purpose — the
   * previous attempt at a direct wire format rendered partitions as a <i>path</i> string, which
   * split on exactly this character and had to reject it at write time. The typed JSON partition
   * these types carry has no such hazard.
   */
  @Test
  public void partitionValuesSurviveTheCoderAndReconstructExactly() throws Exception {
    DataFile original = dataFile("/tmp/d-slash.parquet", "a/b");
    DeleteFile originalDv = deletionVector("/tmp/dv-slash.puffin", "/tmp/d-slash.parquet", "a/b");
    ShardDeltaFiles files =
        shard(
            ImmutableList.of(SerializableDataFile.from(original, SPEC)),
            ImmutableList.of(SerializableDeleteFile.from(originalDv, SPEC)));

    Coder<ShardDeltaFiles> coder = coder();
    ShardDeltaFiles decoded =
        CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, files));

    DataFile rebuilt = decoded.getDataFiles().get(0).createDataFile(SPECS);
    assertThat(rebuilt.specId(), equalTo(SPEC.specId()));
    assertThat(rebuilt.partition().get(0, String.class), equalTo("a/b"));
    assertThat(rebuilt.location(), equalTo(original.location()));
    assertThat(rebuilt.recordCount(), equalTo(original.recordCount()));
    // No sequence number is baked in, so the eventual commit assigns one by snapshot inheritance.
    assertThat(rebuilt.dataSequenceNumber(), nullValue());

    DeleteFile rebuiltDv = decoded.getDeleteFiles().get(0).createDeleteFile(SPECS, null);
    assertThat(rebuiltDv.partition().get(0, String.class), equalTo("a/b"));
    assertThat(rebuiltDv.content(), equalTo(FileContent.POSITION_DELETES));
    assertThat(rebuiltDv.format(), equalTo(FileFormat.PUFFIN));
    assertThat(rebuiltDv.referencedDataFile(), equalTo("/tmp/d-slash.parquet"));
    assertThat(rebuiltDv.contentOffset(), equalTo(4L));
    assertThat(rebuiltDv.contentSizeInBytes(), equalTo(40L));
  }
}
