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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Equivalence;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.checkerframework.checker.nullness.qual.Nullable;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SerializableDeleteFile {
  public static SerializableDeleteFile.Builder builder() {
    return new AutoValue_SerializableDeleteFile.Builder();
  }

  @SchemaFieldNumber("0")
  public abstract FileContent getContentType();

  @SchemaFieldNumber("1")
  public abstract String getLocation();

  @SchemaFieldNumber("2")
  public abstract String getFileFormat();

  @SchemaFieldNumber("3")
  public abstract long getRecordCount();

  @SchemaFieldNumber("4")
  public abstract long getFileSizeInBytes();

  @SchemaFieldNumber("5")
  public abstract String getPartitionPath();

  @SchemaFieldNumber("6")
  public abstract int getPartitionSpecId();

  @SchemaFieldNumber("7")
  public abstract @Nullable Integer getSortOrderId();

  @SchemaFieldNumber("8")
  public abstract @Nullable List<Integer> getEqualityFieldIds();

  @SchemaFieldNumber("9")
  public abstract @Nullable ByteBuffer getKeyMetadata();

  @SchemaFieldNumber("10")
  public abstract @Nullable List<Long> getSplitOffsets();

  @SchemaFieldNumber("11")
  public abstract @Nullable Map<Integer, Long> getColumnSizes();

  @SchemaFieldNumber("12")
  public abstract @Nullable Map<Integer, Long> getValueCounts();

  @SchemaFieldNumber("13")
  public abstract @Nullable Map<Integer, Long> getNullValueCounts();

  @SchemaFieldNumber("14")
  public abstract @Nullable Map<Integer, Long> getNanValueCounts();

  @SchemaFieldNumber("15")
  public abstract @Nullable Map<Integer, byte[]> getLowerBounds();

  @SchemaFieldNumber("16")
  public abstract @Nullable Map<Integer, byte[]> getUpperBounds();

  @SchemaFieldNumber("17")
  public abstract @Nullable Long getContentOffset();

  @SchemaFieldNumber("18")
  public abstract @Nullable Long getContentSizeInBytes();

  @SchemaFieldNumber("19")
  public abstract @Nullable String getReferencedDataFile();

  @SchemaFieldNumber("20")
  public abstract @Nullable Long getDataSequenceNumber();

  @SchemaFieldNumber("21")
  public abstract @Nullable Long getFileSequenceNumber();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setContentType(FileContent content);

    abstract Builder setLocation(String path);

    abstract Builder setFileFormat(String fileFormat);

    abstract Builder setRecordCount(long recordCount);

    abstract Builder setFileSizeInBytes(long fileSizeInBytes);

    abstract Builder setPartitionPath(String partitionPath);

    abstract Builder setPartitionSpecId(int partitionSpec);

    abstract Builder setSortOrderId(int sortOrderId);

    abstract Builder setEqualityFieldIds(List<Integer> equalityFieldIds);

    abstract Builder setKeyMetadata(ByteBuffer keyMetadata);

    abstract Builder setSplitOffsets(List<Long> splitOffsets);

    abstract Builder setColumnSizes(Map<Integer, Long> columnSizes);

    abstract Builder setValueCounts(Map<Integer, Long> valueCounts);

    abstract Builder setNullValueCounts(Map<Integer, Long> nullValueCounts);

    abstract Builder setNanValueCounts(Map<Integer, Long> nanValueCounts);

    abstract Builder setLowerBounds(@Nullable Map<Integer, byte[]> lowerBounds);

    abstract Builder setUpperBounds(@Nullable Map<Integer, byte[]> upperBounds);

    abstract Builder setContentOffset(@Nullable Long offset);

    abstract Builder setContentSizeInBytes(@Nullable Long sizeInBytes);

    abstract Builder setReferencedDataFile(@Nullable String dataFile);

    abstract Builder setDataSequenceNumber(@Nullable Long number);

    abstract Builder setFileSequenceNumber(@Nullable Long number);

    abstract SerializableDeleteFile build();
  }

  public static SerializableDeleteFile from(DeleteFile deleteFile, String partitionPath) {
    return SerializableDeleteFile.builder()
        .setLocation(deleteFile.location())
        .setFileFormat(deleteFile.format().name())
        .setFileSizeInBytes(deleteFile.fileSizeInBytes())
        .setPartitionPath(partitionPath)
        .setPartitionSpecId(deleteFile.specId())
        .setRecordCount(deleteFile.recordCount())
        .setColumnSizes(deleteFile.columnSizes())
        .setValueCounts(deleteFile.valueCounts())
        .setNullValueCounts(deleteFile.nullValueCounts())
        .setNanValueCounts(deleteFile.nanValueCounts())
        .setLowerBounds(toByteArrayMap(deleteFile.lowerBounds()))
        .setUpperBounds(toByteArrayMap(deleteFile.upperBounds()))
        .setSplitOffsets(deleteFile.splitOffsets())
        .setKeyMetadata(deleteFile.keyMetadata())
        .setEqualityFieldIds(deleteFile.equalityFieldIds())
        .setSortOrderId(deleteFile.sortOrderId())
        .setContentOffset(deleteFile.contentOffset())
        .setContentSizeInBytes(deleteFile.contentSizeInBytes())
        .setReferencedDataFile(deleteFile.referencedDataFile())
        .setContentType(deleteFile.content())
        .setDataSequenceNumber(deleteFile.dataSequenceNumber())
        .setFileSequenceNumber(deleteFile.fileSequenceNumber())
        .build();
  }

  @SuppressWarnings("nullness")
  public DeleteFile createDeleteFile(
      Map<Integer, PartitionSpec> partitionSpecs, @Nullable Map<Integer, SortOrder> sortOrders) {
    PartitionSpec partitionSpec =
        checkStateNotNull(
            partitionSpecs.get(getPartitionSpecId()),
            "This DeleteFile was originally created with spec id '%s', "
                + "but table only has spec ids: %s.",
            getPartitionSpecId(),
            partitionSpecs.keySet());

    Metrics metrics =
        new Metrics(
            getRecordCount(),
            getColumnSizes(),
            getValueCounts(),
            getNullValueCounts(),
            getNanValueCounts(),
            toByteBufferMap(getLowerBounds()),
            toByteBufferMap(getUpperBounds()));

    FileMetadata.Builder deleteFileBuilder =
        FileMetadata.deleteFileBuilder(partitionSpec)
            .withPath(getLocation())
            .withFormat(getFileFormat())
            .withFileSizeInBytes(getFileSizeInBytes())
            .withRecordCount(getRecordCount())
            .withMetrics(metrics)
            .withSplitOffsets(getSplitOffsets())
            .withEncryptionKeyMetadata(getKeyMetadata())
            .withPartitionPath(getPartitionPath());

    switch (getContentType()) {
      case POSITION_DELETES:
        deleteFileBuilder = deleteFileBuilder.ofPositionDeletes();
        break;
      case EQUALITY_DELETES:
        int[] equalityFieldIds =
            Objects.requireNonNullElse(getEqualityFieldIds(), new ArrayList<Integer>()).stream()
                .mapToInt(Integer::intValue)
                .toArray();
        SortOrder sortOrder = SortOrder.unsorted();
        if (sortOrders != null) {
          sortOrder =
              checkStateNotNull(
                  sortOrders.get(getSortOrderId()),
                  "This DeleteFile was originally created with sort order id '%s', "
                      + "but table only has sort order ids: %s.",
                  getSortOrderId(),
                  sortOrders.keySet());
        }
        deleteFileBuilder =
            deleteFileBuilder.ofEqualityDeletes(equalityFieldIds).withSortOrder(sortOrder);
        break;
      default:
        throw new IllegalStateException(
            "Unexpected content type for DeleteFile: " + getContentType());
    }

    // needed for puffin files
    if (getFileFormat().equalsIgnoreCase(FileFormat.PUFFIN.name())) {
      deleteFileBuilder =
          deleteFileBuilder
              .withContentOffset(checkStateNotNull(getContentOffset()))
              .withContentSizeInBytes(checkStateNotNull(getContentSizeInBytes()))
              .withReferencedDataFile(checkStateNotNull(getReferencedDataFile()));
    }
    return deleteFileBuilder.build();
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

  @Override
  public final boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SerializableDeleteFile that = (SerializableDeleteFile) o;
    return getContentType().equals(that.getContentType())
        && getLocation().equals(that.getLocation())
        && getFileFormat().equals(that.getFileFormat())
        && getRecordCount() == that.getRecordCount()
        && getFileSizeInBytes() == that.getFileSizeInBytes()
        && getPartitionPath().equals(that.getPartitionPath())
        && getPartitionSpecId() == that.getPartitionSpecId()
        && Objects.equals(getSortOrderId(), that.getSortOrderId())
        && Objects.equals(getEqualityFieldIds(), that.getEqualityFieldIds())
        && Objects.equals(getKeyMetadata(), that.getKeyMetadata())
        && Objects.equals(getSplitOffsets(), that.getSplitOffsets())
        && Objects.equals(getColumnSizes(), that.getColumnSizes())
        && Objects.equals(getValueCounts(), that.getValueCounts())
        && Objects.equals(getNullValueCounts(), that.getNullValueCounts())
        && Objects.equals(getNanValueCounts(), that.getNanValueCounts())
        && mapEquals(getLowerBounds(), that.getLowerBounds())
        && mapEquals(getUpperBounds(), that.getUpperBounds())
        && Objects.equals(getContentOffset(), that.getContentOffset())
        && Objects.equals(getContentSizeInBytes(), that.getContentSizeInBytes())
        && Objects.equals(getReferencedDataFile(), that.getReferencedDataFile())
        && Objects.equals(getDataSequenceNumber(), that.getDataSequenceNumber())
        && Objects.equals(getFileSequenceNumber(), that.getFileSequenceNumber());
  }

  private static boolean mapEquals(
      @Nullable Map<Integer, byte[]> map1, @Nullable Map<Integer, byte[]> map2) {
    if (map1 == null && map2 == null) {
      return true;
    } else if (map1 == null || map2 == null) {
      return false;
    }
    Equivalence<byte[]> byteArrayEquivalence =
        new Equivalence<byte[]>() {
          @Override
          protected boolean doEquivalent(byte[] a, byte[] b) {
            return Arrays.equals(a, b);
          }

          @Override
          protected int doHash(byte[] bytes) {
            return Arrays.hashCode(bytes);
          }
        };

    return Maps.difference(map1, map2, byteArrayEquivalence).areEqual();
  }

  @Override
  public final int hashCode() {
    int hashCode =
        Objects.hash(
            getContentType(),
            getLocation(),
            getFileFormat(),
            getRecordCount(),
            getFileSizeInBytes(),
            getPartitionPath(),
            getPartitionSpecId(),
            getSortOrderId(),
            getEqualityFieldIds(),
            getKeyMetadata(),
            getSplitOffsets(),
            getColumnSizes(),
            getValueCounts(),
            getNullValueCounts(),
            getNanValueCounts(),
            getContentOffset(),
            getContentSizeInBytes(),
            getReferencedDataFile(),
            getDataSequenceNumber(),
            getFileSequenceNumber());
    hashCode = 31 * hashCode + computeMapByteHashCode(getLowerBounds());
    hashCode = 31 * hashCode + computeMapByteHashCode(getUpperBounds());
    return hashCode;
  }

  private static int computeMapByteHashCode(@Nullable Map<Integer, byte[]> map) {
    if (map == null) {
      return 0;
    }
    int hashCode = 0;
    for (Map.Entry<Integer, byte[]> entry : map.entrySet()) {
      int keyHash = entry.getKey().hashCode();
      int valueHash = Arrays.hashCode(entry.getValue()); // content-based hash code
      hashCode += keyHash ^ valueHash;
    }
    return hashCode;
  }
}
