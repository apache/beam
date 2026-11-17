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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.TypeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An encodable description of an Iceberg manifest file, used to distribute manifest reads across
 * workers.
 *
 * <p>Carries exactly the manifest properties that Iceberg's manifest readers use, plus fields
 * needed by maintenance operations. Partition field summaries and file/row counts are intentionally
 * omitted.
 *
 * <p>When a manifest's partition spec may no longer be present in table metadata (for example,
 * after expired metadata is cleaned up), the spec can be attached with {@link
 * #withPartitionSpec(PartitionSpec)} so that the manifest can still be read.
 *
 * <p>Use {@link #from(String, ManifestFile, boolean)} to create instances.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
@Internal
public abstract class ManifestDescriptor {

  /** The identifier of the table the manifest belongs to. */
  @SchemaFieldNumber("0")
  public abstract String getTableIdentifier();

  /** The manifest file location. */
  @SchemaFieldNumber("1")
  public abstract String getPath();

  /** The manifest file length in bytes. */
  @SchemaFieldNumber("2")
  public abstract long getLength();

  /** The ID of the partition spec the manifest was written with. */
  @SchemaFieldNumber("3")
  public abstract int getPartitionSpecId();

  /** Whether the manifest tracks data files or delete files. */
  @SchemaFieldNumber("4")
  public abstract ManifestContent getContent();

  /** The ID of the snapshot that added the manifest. */
  @SchemaFieldNumber("5")
  public abstract long getSnapshotId();

  /** The sequence number of the commit that added the manifest. */
  @SchemaFieldNumber("6")
  public abstract long getSequenceNumber();

  /** The first row ID assigned to the manifest's data files (format v3+), if any. */
  @SchemaFieldNumber("7")
  public abstract @Nullable Long getFirstRowId();

  /**
   * The encryption key metadata of the manifest, if the table is encrypted.
   *
   * <p>Always backed by an array that exactly holds the key metadata. Callers must not modify the
   * returned buffer or its contents.
   */
  @SchemaFieldNumber("8")
  public abstract @Nullable ByteBuffer getKeyMetadata();

  /**
   * Whether the files referenced by the manifest are reachable from table state that must be
   * preserved. Copied onto every {@link FileInfo} read from the manifest.
   */
  @SchemaFieldNumber("9")
  public abstract boolean getReachable();

  /** The JSON-serialized partition spec attached by {@link #withPartitionSpec}, if any. */
  @SchemaFieldNumber("10")
  abstract @Nullable String getPartitionSpecJson();

  /**
   * The JSON-serialized schema that the attached partition spec is bound to, pruned to the spec's
   * source fields, if any.
   */
  @SchemaFieldNumber("11")
  abstract @Nullable String getPartitionSchemaJson();

  /**
   * Creates a descriptor of {@code manifest}.
   *
   * @throws IllegalArgumentException if the manifest has no snapshot ID, which Iceberg requires in
   *     order to read it
   */
  public static ManifestDescriptor from(
      String tableIdentifier, ManifestFile manifest, boolean reachable) {
    Long snapshotId = manifest.snapshotId();
    checkArgument(
        snapshotId != null,
        "Cannot describe manifest %s: it has no snapshot ID, so it cannot be read",
        manifest.path());
    return builder()
        .setTableIdentifier(tableIdentifier)
        .setPath(manifest.path())
        .setLength(manifest.length())
        .setPartitionSpecId(manifest.partitionSpecId())
        .setContent(manifest.content())
        .setSnapshotId(snapshotId)
        .setSequenceNumber(manifest.sequenceNumber())
        .setFirstRowId(manifest.firstRowId())
        .setKeyMetadata(copyOf(manifest.keyMetadata()))
        .setReachable(reachable)
        .build();
  }

  /**
   * Returns a copy of this descriptor that carries {@code spec}, so the manifest can be read even
   * if the spec is no longer present in table metadata.
   *
   * <p>Only the schema fields that {@code spec} is derived from are carried, so the encoded size
   * does not grow with the width of the table schema.
   *
   * @throws IllegalArgumentException if {@code spec} is not the spec the manifest was written with
   */
  public ManifestDescriptor withPartitionSpec(PartitionSpec spec) {
    checkArgument(
        spec.specId() == getPartitionSpecId(),
        "Partition spec %s does not match spec %s of manifest %s",
        spec.specId(),
        getPartitionSpecId(),
        getPath());
    Set<Integer> sourceIds = new HashSet<>();
    for (PartitionField field : spec.fields()) {
      sourceIds.add(field.sourceId());
    }
    return toBuilder()
        .setPartitionSpecJson(PartitionSpecParser.toJson(spec))
        .setPartitionSchemaJson(SchemaParser.toJson(TypeUtil.select(spec.schema(), sourceIds)))
        .build();
  }

  /**
   * Returns the partition spec attached by {@link #withPartitionSpec}, or {@code null} if none was
   * attached.
   */
  public @Nullable PartitionSpec partitionSpec() {
    String specJson = getPartitionSpecJson();
    String schemaJson = getPartitionSchemaJson();
    if (specJson == null || schemaJson == null) {
      return null;
    }
    return PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaJson), specJson);
  }

  /**
   * Copies the remaining bytes of {@code buffer} into a new array-backed buffer.
   *
   * <p>Beam encodes {@link ByteBuffer} schema fields from {@link ByteBuffer#array()}, so the buffer
   * must be writable, heap-backed, and hold exactly the key metadata. Buffers returned by Iceberg
   * may be read-only or views into a larger array.
   */
  private static @Nullable ByteBuffer copyOf(@Nullable ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    ByteBuffer source = buffer.duplicate();
    byte[] bytes = new byte[source.remaining()];
    source.get(bytes);
    return ByteBuffer.wrap(bytes);
  }

  static Builder builder() {
    return new AutoValue_ManifestDescriptor.Builder();
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTableIdentifier(String tableIdentifier);

    abstract Builder setPath(String path);

    abstract Builder setLength(long length);

    abstract Builder setPartitionSpecId(int partitionSpecId);

    abstract Builder setContent(ManifestContent content);

    abstract Builder setSnapshotId(long snapshotId);

    abstract Builder setSequenceNumber(long sequenceNumber);

    abstract Builder setFirstRowId(@Nullable Long firstRowId);

    abstract Builder setKeyMetadata(@Nullable ByteBuffer keyMetadata);

    abstract Builder setReachable(boolean reachable);

    abstract Builder setPartitionSpecJson(@Nullable String partitionSpecJson);

    abstract Builder setPartitionSchemaJson(@Nullable String partitionSchemaJson);

    abstract ManifestDescriptor build();
  }
}
