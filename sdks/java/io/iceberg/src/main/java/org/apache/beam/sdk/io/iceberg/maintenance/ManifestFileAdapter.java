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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A read-only {@link ManifestFile} view of a {@link ManifestDescriptor}, used to open the manifest
 * with Iceberg's manifest readers.
 *
 * <p>Only the properties used by {@link org.apache.iceberg.ManifestFiles#read} and {@link
 * org.apache.iceberg.ManifestFiles#readDeleteManifest} are supported. The descriptor does not carry
 * file/row counts, partition summaries, or the minimum sequence number, so those methods throw
 * {@link UnsupportedOperationException} rather than returning values that could be mistaken for
 * real metadata. If a future Iceberg version starts using one of them on the read path, reads fail
 * loudly instead of silently misbehaving. The inherited {@code has*Files()} defaults call the count
 * methods and therefore throw as well.
 *
 * <p>The inherited {@link #formatVersion()} and {@link #manifestDeletionVector()} defaults are
 * kept; they are only used by format v4 readers, and {@link ReadManifests} rejects format v4
 * tables.
 */
class ManifestFileAdapter implements ManifestFile {
  private final ManifestDescriptor descriptor;

  ManifestFileAdapter(ManifestDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public String path() {
    return descriptor.getPath();
  }

  @Override
  public long length() {
    return descriptor.getLength();
  }

  @Override
  public int partitionSpecId() {
    return descriptor.getPartitionSpecId();
  }

  @Override
  public ManifestContent content() {
    return descriptor.getContent();
  }

  @Override
  public long sequenceNumber() {
    return descriptor.getSequenceNumber();
  }

  @Override
  public Long snapshotId() {
    return descriptor.getSnapshotId();
  }

  /**
   * Returns a duplicate of the descriptor's key metadata, so readers cannot move the position of
   * the descriptor's buffer.
   */
  @Override
  @SuppressWarnings("nullness") // Iceberg documents a null return for unencrypted manifests.
  public @Nullable ByteBuffer keyMetadata() {
    ByteBuffer keyMetadata = descriptor.getKeyMetadata();
    return keyMetadata == null ? null : keyMetadata.duplicate();
  }

  @Override
  @SuppressWarnings("nullness") // Iceberg documents a null return when row lineage is unset.
  public @Nullable Long firstRowId() {
    return descriptor.getFirstRowId();
  }

  @Override
  public long minSequenceNumber() {
    throw unsupported("minSequenceNumber");
  }

  @Override
  public Integer addedFilesCount() {
    throw unsupported("addedFilesCount");
  }

  @Override
  public Long addedRowsCount() {
    throw unsupported("addedRowsCount");
  }

  @Override
  public Integer existingFilesCount() {
    throw unsupported("existingFilesCount");
  }

  @Override
  public Long existingRowsCount() {
    throw unsupported("existingRowsCount");
  }

  @Override
  public Integer deletedFilesCount() {
    throw unsupported("deletedFilesCount");
  }

  @Override
  public Long deletedRowsCount() {
    throw unsupported("deletedRowsCount");
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    throw unsupported("partitions");
  }

  /** Returns this adapter, which is immutable. */
  @Override
  public ManifestFile copy() {
    return this;
  }

  @Override
  public String toString() {
    return "ManifestFileAdapter{path="
        + descriptor.getPath()
        + ", content="
        + descriptor.getContent()
        + ", partitionSpecId="
        + descriptor.getPartitionSpecId()
        + ", table="
        + descriptor.getTableIdentifier()
        + "}";
  }

  private static UnsupportedOperationException unsupported(String method) {
    return new UnsupportedOperationException(
        method + "() is not carried by ManifestDescriptor and is not needed to read a manifest");
  }
}
