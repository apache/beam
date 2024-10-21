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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Serializable version of an Iceberg {@link DataFile}.
 *
 * <p>{@link DataFile} is not serializable and the Iceberg API doesn't offer an easy way to
 * encode/decode it. This class is an identical version that can be used as a PCollection element
 * type.
 *
 * <p>Use {@link #from(DataFile, PartitionKey)} to create a {@link SerializableDataFile} and {@link
 * #createDataFile(PartitionSpec)} to reconstruct the original {@link DataFile}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class SerializableDataFile {
  public static Builder builder() {
    return new AutoValue_SerializableDataFile.Builder();
  }

  abstract String getPath();

  abstract String getFileFormat();

  abstract long getRecordCount();

  abstract long getFileSizeInBytes();

  abstract String getPartitionPath();

  abstract int getPartitionSpecId();

  abstract @Nullable ByteBuffer getKeyMetadata();

  abstract @Nullable List<Long> getSplitOffsets();

  abstract @Nullable Map<Integer, Long> getColumnSizes();

  abstract @Nullable Map<Integer, Long> getValueCounts();

  abstract @Nullable Map<Integer, Long> getNullValueCounts();

  abstract @Nullable Map<Integer, Long> getNanValueCounts();

  abstract @Nullable Map<Integer, byte[]> getLowerBounds();

  abstract @Nullable Map<Integer, byte[]> getUpperBounds();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setPath(String path);

    abstract Builder setFileFormat(String fileFormat);

    abstract Builder setRecordCount(long recordCount);

    abstract Builder setFileSizeInBytes(long fileSizeInBytes);

    abstract Builder setPartitionPath(String partitionPath);

    abstract Builder setPartitionSpecId(int partitionSpec);

    abstract Builder setKeyMetadata(ByteBuffer keyMetadata);

    abstract Builder setSplitOffsets(List<Long> splitOffsets);

    abstract Builder setColumnSizes(Map<Integer, Long> columnSizes);

    abstract Builder setValueCounts(Map<Integer, Long> valueCounts);

    abstract Builder setNullValueCounts(Map<Integer, Long> nullValueCounts);

    abstract Builder setNanValueCounts(Map<Integer, Long> nanValueCounts);

    abstract Builder setLowerBounds(@Nullable Map<Integer, byte[]> lowerBounds);

    abstract Builder setUpperBounds(@Nullable Map<Integer, byte[]> upperBounds);

    abstract SerializableDataFile build();
  }

  /**
   * Create a {@link SerializableDataFile} from a {@link DataFile} and its associated {@link
   * PartitionKey}.
   */
  static SerializableDataFile from(DataFile f, PartitionKey key) {
    return SerializableDataFile.builder()
        .setPath(f.path().toString())
        .setFileFormat(f.format().toString())
        .setRecordCount(f.recordCount())
        .setFileSizeInBytes(f.fileSizeInBytes())
        .setPartitionPath(key.toPath())
        .setPartitionSpecId(f.specId())
        .setKeyMetadata(f.keyMetadata())
        .setSplitOffsets(f.splitOffsets())
        .setColumnSizes(f.columnSizes())
        .setValueCounts(f.valueCounts())
        .setNullValueCounts(f.nullValueCounts())
        .setNanValueCounts(f.nanValueCounts())
        .setLowerBounds(toByteArrayMap(f.lowerBounds()))
        .setUpperBounds(toByteArrayMap(f.upperBounds()))
        .build();
  }

  /**
   * Reconstructs the original {@link DataFile} from this {@link SerializableDataFile}.
   *
   * <p>We require an input {@link PartitionSpec} as well because there's no easy way to reconstruct
   * it from Beam-compatible types.
   */
  @SuppressWarnings("nullness")
  DataFile createDataFile(Map<Integer, PartitionSpec> partitionSpecs) {
    PartitionSpec partitionSpec =
        checkStateNotNull(
            partitionSpecs.get(getPartitionSpecId()),
            "This DataFile was originally created with spec id '%s'. Could not find "
                + "this among table's partition specs: %s.",
            getPartitionSpecId(),
            partitionSpecs.keySet());

    Metrics dataFileMetrics =
        new Metrics(
            getRecordCount(),
            getColumnSizes(),
            getValueCounts(),
            getNullValueCounts(),
            getNanValueCounts(),
            toByteBufferMap(getLowerBounds()),
            toByteBufferMap(getUpperBounds()));

    return DataFiles.builder(partitionSpec)
        .withFormat(FileFormat.fromString(getFileFormat()))
        .withPath(getPath())
        .withPartitionPath(getPartitionPath())
        .withEncryptionKeyMetadata(getKeyMetadata())
        .withFileSizeInBytes(getFileSizeInBytes())
        .withMetrics(dataFileMetrics)
        .withSplitOffsets(getSplitOffsets())
        .build();
  }

  // ByteBuddyUtils has trouble converting Map value type ByteBuffer
  // to byte[] and back to ByteBuffer, so we perform these conversions manually
  // TODO(https://github.com/apache/beam/issues/32701)
  private static @Nullable Map<Integer, byte[]> toByteArrayMap(
      @Nullable Map<Integer, ByteBuffer> input) {
    if (input == null) {
      return null;
    }
    Map<Integer, byte[]> output = new HashMap<>(input.size());
    for (Map.Entry<Integer, ByteBuffer> e : input.entrySet()) {
      output.put(e.getKey(), e.getValue().array());
    }
    return output;
  }

  private static @Nullable Map<Integer, ByteBuffer> toByteBufferMap(
      @Nullable Map<Integer, byte[]> input) {
    if (input == null) {
      return null;
    }
    Map<Integer, ByteBuffer> output = new HashMap<>(input.size());
    for (Map.Entry<Integer, byte[]> e : input.entrySet()) {
      output.put(e.getKey(), ByteBuffer.wrap(e.getValue()));
    }
    return output;
  }
}
