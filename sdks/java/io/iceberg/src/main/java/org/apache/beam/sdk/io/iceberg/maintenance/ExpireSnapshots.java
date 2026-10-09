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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Distributed snapshot expiration maintenance operation for Apache Iceberg tables.
 *
 * <p>Prunes snapshots older than a configured retention threshold, retains the last $N$ ancestors,
 * removes unreferenced manifests and manifest lists, and physically deletes obsolete data and
 * delete files from storage.
 *
 * <h2>Execution Model</h2>
 *
 * <ol>
 *   <li><b>Phase 1 (Planning)</b>: Runs inside an initial worker {@link PlanExpireSnapshotsDoFn}.
 *       Validates table GC settings and commits the metadata change via {@code
 *       table.expireSnapshots().cleanExpiredFiles(false).commit()}. Zero file deletions happen on
 *       the driver.
 *   <li><b>Phase 2 (Manifest Scanning)</b>: Manifest files from candidate and retained snapshots
 *       are redistributed and read in parallel across workers via {@link ReadManifestDoFn}.
 *   <li><b>Phase 3 (Anti-Join)</b>: Candidate and retained files are keyed by file path and
 *       evaluated in a distributed anti-join. Any file referenced by any active snapshot is
 *       preserved.
 *   <li><b>Phase 4 (Physical Deletion)</b>: Unreferenced files are batched and deleted in parallel
 *       across workers via key-sharded {@link GroupIntoBatches} and {@link DeleteFilesDoFn},
 *       utilizing bulk object deletion where supported.
 *   <li><b>Phase 5 (Aggregation)</b>: Deletion counts and expired snapshot counts are merged into a
 *       single {@link ExpireSnapshotsResult}.
 * </ol>
 */
public class ExpireSnapshots
    extends PTransform<PCollection<String>, PCollection<ExpireSnapshotsResult>> {

  public static final String PREFIX = "[ExpireSnapshots] ";

  private final IcebergCatalogConfig catalogConfig;
  private final Configuration config;

  ExpireSnapshots(IcebergCatalogConfig catalogConfig, Configuration config) {
    this.catalogConfig = catalogConfig;
    this.config = config;
  }

  public static ExpireSnapshots create(IcebergCatalogConfig catalogConfig) {
    return new ExpireSnapshots(catalogConfig, Configuration.builder().build());
  }

  public static ExpireSnapshots create(IcebergCatalogConfig catalogConfig, Configuration config) {
    return new ExpireSnapshots(catalogConfig, config);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.addIfNotNull(
        DisplayData.item("expireOlderThan", config.getExpireOlderThan())
            .withLabel("Expire Older Than (Millis)"));
    builder.add(
        DisplayData.item("retainLast", config.retainLast()).withLabel("Retain Last Snapshots"));
    builder.add(
        DisplayData.item("cleanFiles", config.cleanFiles())
            .withLabel("Clean Files (Physical Deletion)"));
    builder.add(
        DisplayData.item("cleanExpiredMetadata", config.cleanExpiredMetadata())
            .withLabel("Clean Expired Metadata"));
    builder.add(
        DisplayData.item("deleteBatchSize", config.deleteBatchSize())
            .withLabel("Delete Batch Size"));
  }

  @Override
  public PCollection<ExpireSnapshotsResult> expand(PCollection<String> tableIdentifiers) {
    Preconditions.checkArgument(
        tableIdentifiers.isBounded() == PCollection.IsBounded.BOUNDED,
        "ExpireSnapshots only supports bounded (batch) input.");
    config.validate();

    SchemaCoder<FileInfo> fileInfoCoder;
    SchemaCoder<ExpireSnapshotsResult> resultCoder;
    try {
      fileInfoCoder =
          tableIdentifiers.getPipeline().getSchemaRegistry().getSchemaCoder(FileInfo.class);
      resultCoder =
          tableIdentifiers
              .getPipeline()
              .getSchemaRegistry()
              .getSchemaCoder(ExpireSnapshotsResult.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Failed to load schema coders for ExpireSnapshots", e);
    }

    KvCoder<String, FileInfo> kvFileInfoCoder = KvCoder.of(StringUtf8Coder.of(), fileInfoCoder);

    // Phase 1: Planning and metadata commit
    PCollectionTuple planned =
        tableIdentifiers.apply(
            "Plan Expire Snapshots",
            ParDo.of(new PlanExpireSnapshotsDoFn(catalogConfig, config))
                .withOutputTags(
                    PlanExpireSnapshotsDoFn.PLAN_SUMMARY,
                    TupleTagList.of(PlanExpireSnapshotsDoFn.MANIFESTS)
                        .and(PlanExpireSnapshotsDoFn.DIRECT_FILES)));

    PCollection<ExpireSnapshotsResult> planSummary =
        planned.get(PlanExpireSnapshotsDoFn.PLAN_SUMMARY).setCoder(resultCoder);

    PCollection<KV<String, FileInfo>> directFiles =
        planned.get(PlanExpireSnapshotsDoFn.DIRECT_FILES).setCoder(kvFileInfoCoder);

    // Phase 2: Distributed manifest scanning
    PCollection<KV<String, FileInfo>> manifestEntries =
        planned
            .get(PlanExpireSnapshotsDoFn.MANIFESTS)
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ManifestFileBean.class)))
            .apply("Redistribute Manifests", Redistribute.arbitrarily())
            .apply("Read Manifest Entries", ParDo.of(new ReadManifestDoFn(catalogConfig)))
            .setCoder(kvFileInfoCoder);

    // Phase 3: Distributed anti-join
    PCollection<FileInfo> filesToDelete =
        PCollectionList.of(directFiles)
            .and(manifestEntries)
            .apply("Flatten All Files", Flatten.pCollections())
            .setCoder(kvFileInfoCoder)
            .apply("Group by Path", GroupByKey.create())
            .apply("Anti-Join Filter", ParDo.of(new AntiJoinFilterFn()))
            .setCoder(fileInfoCoder);

    // Phase 4: Batched file deletion
    PCollection<ExpireSnapshotsResult> deletionSummary =
        filesToDelete
            .apply(
                "Key for Batching",
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.strings(),
                            org.apache.beam.sdk.values.TypeDescriptor.of(FileInfo.class)))
                    .via(
                        file ->
                            KV.of(MoreObjects.firstNonNull(file.getTableIdentifier(), ""), file)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), fileInfoCoder))
            .apply(
                "Batch Files",
                GroupIntoBatches.<String, FileInfo>ofSize(config.deleteBatchSize())
                    .withShardedKey())
            .setCoder(
                KvCoder.of(
                    ShardedKey.Coder.of(StringUtf8Coder.of()), IterableCoder.of(fileInfoCoder)))
            .apply("Delete Files", ParDo.of(new DeleteFilesDoFn(catalogConfig, config)))
            .setCoder(resultCoder);

    // Phase 5: Global metric aggregation
    return PCollectionList.of(planSummary)
        .and(deletionSummary)
        .apply("Flatten Result Fragments", Flatten.pCollections())
        .setCoder(resultCoder)
        .apply("Merge into Final Result", Combine.globally(new ExpireSnapshotsResult.Merge()));
  }

  /** Filters grouped file entries: emits candidate if NO valid reference exists. */
  static class AntiJoinFilterFn extends DoFn<KV<String, Iterable<FileInfo>>, FileInfo> {
    @ProcessElement
    public void process(
        @Element KV<String, Iterable<FileInfo>> element, OutputReceiver<FileInfo> out) {
      boolean isValid = false;
      FileInfo candidate = null;

      for (FileInfo info : element.getValue()) {
        if (info.getValid()) {
          isValid = true;
          break;
        }
        if (candidate == null) {
          candidate = info;
        }
      }

      if (!isValid && candidate != null) {
        out.output(candidate);
      }
    }
  }

  /** Configuration options for {@link ExpireSnapshots}. */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Configuration implements Serializable {

    public static Builder builder() {
      return new AutoValue_ExpireSnapshots_Configuration.Builder()
          .setRetainLast(1)
          .setCleanFiles(true)
          .setDeleteBatchSize(10_000);
    }

    @SchemaFieldDescription(
        "Cutoff timestamp in milliseconds. Snapshots older than this are expired.")
    @Pure
    public abstract @Nullable Long getExpireOlderThan();

    @SchemaFieldDescription(
        "Safety floor: minimum number of ancestor snapshots to retain. Must be >= 1.")
    @Pure
    public abstract @Nullable Integer getRetainLast();

    @SchemaFieldDescription("Explicit snapshot IDs to expire.")
    @Pure
    public abstract @Nullable List<Long> getSnapshotIds();

    @SchemaFieldDescription(
        "Whether to clean up unused partition specs and schemas no longer referenced by any snapshot.")
    @Pure
    public abstract @Nullable Boolean getCleanExpiredMetadata();

    @SchemaFieldDescription(
        "Whether to physically delete unreferenced files from storage. If false, acts as a dry run.")
    @Pure
    public abstract @Nullable Boolean getCleanFiles();

    @SchemaFieldDescription("Batch size for bulk file deletion calls. Default is 10,000.")
    @Pure
    public abstract @Nullable Integer getDeleteBatchSize();

    public int retainLast() {
      return MoreObjects.firstNonNull(getRetainLast(), 1);
    }

    public boolean cleanExpiredMetadata() {
      return MoreObjects.firstNonNull(getCleanExpiredMetadata(), false);
    }

    public boolean cleanFiles() {
      return MoreObjects.firstNonNull(getCleanFiles(), true);
    }

    public int deleteBatchSize() {
      return MoreObjects.firstNonNull(getDeleteBatchSize(), 10_000);
    }

    public void validate() {
      if (getRetainLast() != null) {
        Preconditions.checkArgument(
            getRetainLast() >= 1,
            "retainLast must be at least 1 to prevent deleting current table state, got %s",
            getRetainLast());
      }
      if (getDeleteBatchSize() != null) {
        Preconditions.checkArgument(
            getDeleteBatchSize() > 0,
            "deleteBatchSize must be positive, got %s",
            getDeleteBatchSize());
      }
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setExpireOlderThan(@Nullable Long millis);

      public abstract Builder setRetainLast(@Nullable Integer retainLast);

      public abstract Builder setSnapshotIds(@Nullable List<Long> snapshotIds);

      public abstract Builder setCleanExpiredMetadata(@Nullable Boolean clean);

      public abstract Builder setCleanFiles(@Nullable Boolean cleanFiles);

      public abstract Builder setDeleteBatchSize(@Nullable Integer size);

      public abstract Configuration build();
    }
  }
}
