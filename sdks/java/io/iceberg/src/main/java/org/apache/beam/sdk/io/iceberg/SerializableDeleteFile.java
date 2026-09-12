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

import static org.apache.beam.sdk.io.iceberg.SerializableDataFile.computeMapByteHashCode;
import static org.apache.beam.sdk.io.iceberg.SerializableDataFile.mapEquals;
import static org.apache.beam.sdk.io.iceberg.SerializableDataFile.toByteArrayMap;
import static org.apache.beam.sdk.io.iceberg.SerializableDataFile.toByteBufferMap;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.checkerframework.checker.nullness.qual.Nullable;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
@Internal
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

  /**
   * @deprecated Use {@link #getJsonPartition()} instead.
   */
  @SchemaFieldNumber("5")
  @Deprecated
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

  @SchemaFieldNumber("22")
  abstract @Nullable String getJsonPartition();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setContentType(FileContent content);

    abstract Builder setLocation(String path);

    abstract Builder setFileFormat(String fileFormat);

    abstract Builder setRecordCount(long recordCount);

    abstract Builder setFileSizeInBytes(long fileSizeInBytes);

    abstract Builder setPartitionPath(String partitionPath);

    abstract Builder setJsonPartition(String jsonPartition);

    abstract Builder setPartitionSpecId(int partitionSpec);

    abstract Builder setSortOrderId(@Nullable Integer sortOrderId);

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

  public static SerializableDeleteFile from(
      DeleteFile deleteFile, Map<Integer, PartitionSpec> specs) {
    return from(deleteFile, specs, true);
  }

  /**
   * Creates a {@link SerializableDeleteFile}, resolving the file's {@link PartitionSpec} by its own
   * spec id.
   *
   * <p>Delete files reached from a scan task may carry a spec id that differs from the spec of the
   * data file they apply to, so the lookup has to be per delete file rather than against a single
   * "current" spec.
   */
  public static SerializableDeleteFile from(
      DeleteFile deleteFile, Map<Integer, PartitionSpec> specs, boolean includeMetrics) {
    return from(
        deleteFile,
        checkStateNotNull(
            specs.get(deleteFile.specId()),
            "Could not create a SerializableDeleteFile because DeleteFile is written using a partition spec id '%s' that is not found in the provided specs: %s",
            deleteFile.specId(),
            specs.keySet()),
        includeMetrics);
  }

  public static SerializableDeleteFile from(DeleteFile deleteFile, PartitionSpec spec) {
    return from(deleteFile, spec, true);
  }

  public static SerializableDeleteFile from(
      DeleteFile deleteFile, PartitionSpec spec, boolean includeMetrics) {
    if (spec.specId() != deleteFile.specId()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot serialize DeleteFile: its partition spec id %s does not match the provided "
                  + "spec id %s.",
              deleteFile.specId(), spec.specId()));
    }
    // jsonPartition is the primary (handles evolved specs, special characters).
    // partitionPath is the fallback for values that don't round-trip through JSON.
    String jsonPartition = SingleValueParser.toJson(spec.partitionType(), deleteFile.partition());
    String partitionPath = spec.partitionToPath(deleteFile.partition());

    SerializableDeleteFile.Builder builder =
        SerializableDeleteFile.builder()
            .setLocation(deleteFile.location())
            .setFileFormat(deleteFile.format().name())
            .setFileSizeInBytes(deleteFile.fileSizeInBytes())
            .setPartitionPath(partitionPath)
            .setJsonPartition(jsonPartition)
            .setPartitionSpecId(deleteFile.specId())
            .setRecordCount(deleteFile.recordCount())
            .setColumnSizes(deleteFile.columnSizes())
            .setValueCounts(deleteFile.valueCounts())
            .setNullValueCounts(deleteFile.nullValueCounts())
            .setNanValueCounts(deleteFile.nanValueCounts())
            .setSplitOffsets(deleteFile.splitOffsets())
            .setKeyMetadata(deleteFile.keyMetadata())
            .setEqualityFieldIds(deleteFile.equalityFieldIds())
            .setSortOrderId(deleteFile.sortOrderId())
            .setContentOffset(deleteFile.contentOffset())
            .setContentSizeInBytes(deleteFile.contentSizeInBytes())
            .setReferencedDataFile(deleteFile.referencedDataFile())
            .setContentType(deleteFile.content())
            .setDataSequenceNumber(deleteFile.dataSequenceNumber())
            .setFileSequenceNumber(deleteFile.fileSequenceNumber());

    if (includeMetrics) {
      builder =
          builder
              .setLowerBounds(toByteArrayMap(deleteFile.lowerBounds()))
              .setUpperBounds(toByteArrayMap(deleteFile.upperBounds()));
    }

    return builder.build();
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
            .withReferencedDataFile(getReferencedDataFile());

    @Nullable String jsonPartition = getJsonPartition();
    if (jsonPartition != null) {
      try {
        deleteFileBuilder = deleteFileBuilder.withPartition(partition(partitionSpec));
      } catch (RuntimeException e) {
        // Some partition values (e.g. NaN / Infinity floating-point) don't round-trip through the
        // JSON representation; fall back to the partition-path string
        deleteFileBuilder = deleteFileBuilder.withPartitionPath(getPartitionPath());
      }
    } else {
      // Elements decoded from a pre-jsonPartition release carry only the partition path.
      deleteFileBuilder = deleteFileBuilder.withPartitionPath(getPartitionPath());
    }

    switch (getContentType()) {
      case POSITION_DELETES:
        deleteFileBuilder = deleteFileBuilder.ofPositionDeletes();
        break;
      case EQUALITY_DELETES:
        List<Integer> fieldIds = getEqualityFieldIds();
        int[] equalityFieldIds = new int[fieldIds != null ? fieldIds.size() : 0];
        if (fieldIds != null) {
          for (int i = 0; i < fieldIds.size(); i++) {
            equalityFieldIds[i] = fieldIds.get(i);
          }
        }
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

    // contentOffset / contentSizeInBytes really are Puffin-only: build() rejects a non-null value
    // for either on any other format, and requires both (plus referencedDataFile) on Puffin.
    if (getFileFormat().equalsIgnoreCase(FileFormat.PUFFIN.name())) {
      deleteFileBuilder =
          deleteFileBuilder
              .withContentOffset(checkStateNotNull(getContentOffset()))
              .withContentSizeInBytes(checkStateNotNull(getContentSizeInBytes()));
    }
    return deleteFileBuilder.build();
  }

  private StructLike partition(PartitionSpec spec) {
    return (StructLike)
        SingleValueParser.fromJson(spec.partitionType(), checkStateNotNull(getJsonPartition()));
  }

  @Override
  public final boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SerializableDeleteFile)) {
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
        && Objects.equals(getJsonPartition(), that.getJsonPartition())
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
            getJsonPartition(),
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
}
