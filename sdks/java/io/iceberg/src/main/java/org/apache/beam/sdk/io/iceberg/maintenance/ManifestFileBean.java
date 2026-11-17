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
package org.apache.beam.sdk.io.iceberg.maintenance;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.util.ByteBuffers;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Lightweight serializable implementation of {@link ManifestFile} used to fan out manifest reading
 * across Beam workers.
 */
@SuppressWarnings("nullness")
public class ManifestFileBean implements ManifestFile, Serializable {

  private final String path;
  private final long length;
  private final int partitionSpecId;
  private final ManifestContent content;
  private final @Nullable Long snapshotId;
  private final @Nullable Integer addedFilesCount;
  private final @Nullable Long addedRowsCount;
  private final @Nullable Integer existingFilesCount;
  private final @Nullable Long existingRowsCount;
  private final @Nullable Integer deletedFilesCount;
  private final @Nullable Long deletedRowsCount;
  private final long sequenceNumber;
  private final long minSequenceNumber;
  private final boolean valid;
  private final byte @Nullable [] keyMetadata;
  private final @Nullable String partitionSpecJson;
  private final @Nullable String partitionSchemaJson;

  private transient @Nullable PartitionSpec cachedPartitionSpec;

  public ManifestFileBean(
      String path,
      long length,
      int partitionSpecId,
      ManifestContent content,
      @Nullable Long snapshotId,
      boolean valid) {
    this(
        path,
        length,
        partitionSpecId,
        content,
        snapshotId,
        null,
        null,
        null,
        null,
        null,
        null,
        0L,
        0L,
        valid);
  }

  public ManifestFileBean(
      String path,
      long length,
      int partitionSpecId,
      ManifestContent content,
      @Nullable Long snapshotId,
      @Nullable Integer addedFilesCount,
      @Nullable Long addedRowsCount,
      @Nullable Integer existingFilesCount,
      @Nullable Long existingRowsCount,
      @Nullable Integer deletedFilesCount,
      @Nullable Long deletedRowsCount,
      long sequenceNumber,
      long minSequenceNumber,
      boolean valid) {
    this(
        path,
        length,
        partitionSpecId,
        content,
        snapshotId,
        addedFilesCount,
        addedRowsCount,
        existingFilesCount,
        existingRowsCount,
        deletedFilesCount,
        deletedRowsCount,
        sequenceNumber,
        minSequenceNumber,
        valid,
        null,
        null,
        null);
  }

  private ManifestFileBean(
      String path,
      long length,
      int partitionSpecId,
      ManifestContent content,
      @Nullable Long snapshotId,
      @Nullable Integer addedFilesCount,
      @Nullable Long addedRowsCount,
      @Nullable Integer existingFilesCount,
      @Nullable Long existingRowsCount,
      @Nullable Integer deletedFilesCount,
      @Nullable Long deletedRowsCount,
      long sequenceNumber,
      long minSequenceNumber,
      boolean valid,
      byte @Nullable [] keyMetadata,
      @Nullable String partitionSpecJson,
      @Nullable String partitionSchemaJson) {
    this.path = path;
    this.length = length;
    this.partitionSpecId = partitionSpecId;
    this.content = content;
    this.snapshotId = snapshotId;
    this.addedFilesCount = addedFilesCount;
    this.addedRowsCount = addedRowsCount;
    this.existingFilesCount = existingFilesCount;
    this.existingRowsCount = existingRowsCount;
    this.deletedFilesCount = deletedFilesCount;
    this.deletedRowsCount = deletedRowsCount;
    this.sequenceNumber = sequenceNumber;
    this.minSequenceNumber = minSequenceNumber;
    this.valid = valid;
    this.keyMetadata = keyMetadata;
    this.partitionSpecJson = partitionSpecJson;
    this.partitionSchemaJson = partitionSchemaJson;
  }

  public static ManifestFileBean fromManifestFile(ManifestFile manifest, boolean valid) {
    ByteBuffer manifestKeyMetadata = manifest.keyMetadata();
    return new ManifestFileBean(
        manifest.path(),
        manifest.length(),
        manifest.partitionSpecId(),
        manifest.content() != null ? manifest.content() : ManifestContent.DATA,
        manifest.snapshotId(),
        manifest.addedFilesCount(),
        manifest.addedRowsCount(),
        manifest.existingFilesCount(),
        manifest.existingRowsCount(),
        manifest.deletedFilesCount(),
        manifest.deletedRowsCount(),
        manifest.sequenceNumber(),
        manifest.minSequenceNumber(),
        valid,
        manifestKeyMetadata != null ? ByteBuffers.toByteArray(manifestKeyMetadata) : null,
        null,
        null);
  }

  /**
   * Returns a copy of this bean that carries the given partition spec. Used for manifests whose
   * spec may no longer be present in the table metadata (e.g. after expired metadata cleanup), so
   * that the manifest can still be read on workers.
   */
  public ManifestFileBean withPartitionSpec(PartitionSpec spec) {
    Preconditions.checkArgument(
        spec.specId() == partitionSpecId,
        "Partition spec id %s does not match manifest spec id %s",
        spec.specId(),
        partitionSpecId);
    return new ManifestFileBean(
        path,
        length,
        partitionSpecId,
        content,
        snapshotId,
        addedFilesCount,
        addedRowsCount,
        existingFilesCount,
        existingRowsCount,
        deletedFilesCount,
        deletedRowsCount,
        sequenceNumber,
        minSequenceNumber,
        valid,
        keyMetadata,
        PartitionSpecParser.toJson(spec),
        SchemaParser.toJson(spec.schema()));
  }

  /**
   * Returns the partition spec carried by this bean, or {@code null} if none was attached via
   * {@link #withPartitionSpec(PartitionSpec)}.
   */
  public @Nullable PartitionSpec partitionSpec() {
    if (cachedPartitionSpec == null && partitionSpecJson != null && partitionSchemaJson != null) {
      cachedPartitionSpec =
          PartitionSpecParser.fromJson(
              SchemaParser.fromJson(partitionSchemaJson), partitionSpecJson);
    }
    return cachedPartitionSpec;
  }

  public boolean isValid() {
    return valid;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public int partitionSpecId() {
    return partitionSpecId;
  }

  @Override
  public ManifestContent content() {
    return content;
  }

  @Override
  public @Nullable Long snapshotId() {
    return snapshotId;
  }

  @Override
  public @Nullable Integer addedFilesCount() {
    return addedFilesCount;
  }

  @Override
  public @Nullable Long addedRowsCount() {
    return addedRowsCount;
  }

  @Override
  public @Nullable Integer existingFilesCount() {
    return existingFilesCount;
  }

  @Override
  public @Nullable Long existingRowsCount() {
    return existingRowsCount;
  }

  @Override
  public @Nullable Integer deletedFilesCount() {
    return deletedFilesCount;
  }

  @Override
  public @Nullable Long deletedRowsCount() {
    return deletedRowsCount;
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    return Collections.emptyList();
  }

  @Override
  public @Nullable ByteBuffer keyMetadata() {
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata).asReadOnlyBuffer() : null;
  }

  @Override
  public ManifestFile copy() {
    return this;
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long minSequenceNumber() {
    return minSequenceNumber;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ManifestFileBean)) {
      return false;
    }
    ManifestFileBean that = (ManifestFileBean) o;
    return length == that.length
        && partitionSpecId == that.partitionSpecId
        && valid == that.valid
        && Objects.equals(path, that.path)
        && content == that.content
        && Objects.equals(snapshotId, that.snapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, length, partitionSpecId, content, snapshotId, valid);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", path)
        .add("length", length)
        .add("partitionSpecId", partitionSpecId)
        .add("content", content)
        .add("snapshotId", snapshotId)
        .add("valid", valid)
        .toString();
  }
}
