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

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
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
 * <p>{@link DataFile} is not serializable and Iceberg doesn't offer an easy way to encode/decode
 * it. This class is an identical version that can be used as a PCollection element type. {@link
 * #createDataFile(PartitionSpec)} can be used to reconstruct the original {@link DataFile}.
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

    abstract Builder setLowerBounds(Map<Integer, byte[]> lowerBounds);

    abstract Builder setUpperBounds(Map<Integer, byte[]> upperBounds);

    abstract SerializableDataFile build();
  }

  /**
   * Create a {@link SerializableDataFile} from a {@link DataFile} and its associated {@link
   * PartitionKey}.
   */
  static SerializableDataFile from(DataFile f, PartitionKey key) {
    SerializableDataFile.Builder builder =
        SerializableDataFile.builder()
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
            .setNanValueCounts(f.nanValueCounts());

    // ByteBuddyUtils has trouble converting Map value type ByteBuffer
    // to byte[] and back to ByteBuffer, so we perform this conversion manually
    // here.
    if (f.lowerBounds() != null) {
      Map<Integer, byte[]> lowerBounds = new HashMap<>(f.lowerBounds().size());
      for (Map.Entry<Integer, ByteBuffer> e : f.lowerBounds().entrySet()) {
        lowerBounds.put(e.getKey(), e.getValue().array());
      }
      builder = builder.setLowerBounds(lowerBounds);
    }
    if (f.upperBounds() != null) {
      Map<Integer, byte[]> upperBounds = new HashMap<>(f.upperBounds().size());
      for (Map.Entry<Integer, ByteBuffer> e : f.upperBounds().entrySet()) {
        upperBounds.put(e.getKey(), e.getValue().array());
      }
      builder = builder.setUpperBounds(upperBounds);
    }
    return builder.build();
  }

  /**
   * Reconstructs the original {@link DataFile} from this {@link SerializableDataFile}.
   *
   * <p>We require an input {@link PartitionSpec} as well because there's no easy way to reconstruct
   * it from Beam-compatible types.
   */
  @SuppressWarnings("nullness")
  DataFile createDataFile(PartitionSpec partitionSpec) {
    Preconditions.checkState(
        partitionSpec.specId() == getPartitionSpecId(),
        "Invalid partition spec id '%s'. This DataFile was originally created with spec id '%s'.",
        partitionSpec.specId(),
        getPartitionSpecId());

    // ByteBuddyUtils has trouble converting Map value type ByteBuffer
    // to byte[] and back to ByteBuffer, so we perform this conversion manually
    // here.
    Map<Integer, ByteBuffer> lowerBounds = null;
    Map<Integer, ByteBuffer> upperBounds = null;
    if (getLowerBounds() != null) {
      lowerBounds = new HashMap<>(getLowerBounds().size());
      for (Map.Entry<Integer, byte[]> e : getLowerBounds().entrySet()) {
        lowerBounds.put(e.getKey(), ByteBuffer.wrap(e.getValue()));
      }
    }
    if (getUpperBounds() != null) {
      upperBounds = new HashMap<>(getUpperBounds().size());
      for (Map.Entry<Integer, byte[]> e : getUpperBounds().entrySet()) {
        upperBounds.put(e.getKey(), ByteBuffer.wrap(e.getValue()));
      }
    }

    Metrics dataFileMetrics =
        new Metrics(
            getRecordCount(),
            getColumnSizes(),
            getValueCounts(),
            getNullValueCounts(),
            getNanValueCounts(),
            lowerBounds,
            upperBounds);

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
}
