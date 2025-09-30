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

import static org.apache.beam.sdk.io.iceberg.maintenance.PlanDataFileRewrites.OLD_FILES;
import static org.apache.beam.sdk.io.iceberg.maintenance.PlanDataFileRewrites.REWRITE_GROUPS;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.iceberg.SerializableTable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteDataFiles
    extends PTransform<PCollection<SnapshotInfo>, PCollection<SnapshotInfo>> {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFiles.class);
  public static final String REWRITE_PREFIX =
      IcebergMaintenance.MAINTENANCE_PREFIX + "[RewriteDataFiles] ";
  private final String tableIdentifier;
  private final SerializableTable table;
  private final IcebergCatalogConfig catalogConfig;
  private final Configuration rewriteConfig;
  private final String operationId;

  RewriteDataFiles(
      String tableIdentifier,
      SerializableTable table,
      IcebergCatalogConfig catalogConfig,
      Configuration rewriteConfig,
      String operationId) {
    this.tableIdentifier = tableIdentifier;
    this.table = table;
    this.catalogConfig = catalogConfig;
    this.rewriteConfig = rewriteConfig;
    this.operationId = operationId;
  }

  static RewriteDataFiles create(
      String tableIdentifier,
      SerializableTable table,
      IcebergCatalogConfig catalogConfig,
      @Nullable Configuration configuration,
      String operationId) {
    return new RewriteDataFiles(
        tableIdentifier,
        table,
        catalogConfig,
        configuration != null ? configuration : Configuration.builder().build(),
        operationId);
  }

  @Override
  public PCollection<SnapshotInfo> expand(PCollection<SnapshotInfo> impulse) {
    LOG.info(
        REWRITE_PREFIX + "Running {} task with operation-id: {}",
        RewriteDataFiles.class.getSimpleName(),
        operationId);
    PCollectionTuple plan =
        impulse.apply("Scan and Plan Rewrite", new PlanDataFileRewrites(table, rewriteConfig));
    PCollection<KV<Void, SerializableDataFile>> rewrittenFiles =
        plan.get(REWRITE_GROUPS)
            .apply("Redistribute RewriteFileGroups", Redistribute.arbitrarily())
            .setCoder(FILE_GROUP_CODER)
            .apply("Rewrite Groups", ParDo.of(new RewriteDoFn(table, operationId)))
            .setCoder(SERIALIZABLE_DF_CODER)
            .apply("Add Key to New Files", WithKeys.of((Void) null));

    PCollection<KV<Void, SerializableDataFile>> oldFiles =
        plan.get(OLD_FILES)
            .setCoder(SERIALIZABLE_DF_CODER)
            .apply("Add Key to Old Files", WithKeys.of((Void) null));

    return KeyedPCollectionTuple.of(ADD_DATA_FILES, rewrittenFiles)
        .and(DELETE_DATA_FILES, oldFiles)
        .apply(CoGroupByKey.create())
        .apply(
            "Single Commit",
            ParDo.of(
                new CommitFiles(tableIdentifier, catalogConfig, ADD_DATA_FILES, DELETE_DATA_FILES)))
        .setCoder(SNAPSHOT_CODER);
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Configuration implements Serializable {
    public static Builder builder() {
      return new AutoValue_RewriteDataFiles_Configuration.Builder();
    }

    @SchemaFieldDescription(
        "A snapshot ID used for planning and as the starting snapshot id for commit validation when replacing the files")
    @Pure
    public abstract @Nullable Long getSnapshotId();

    @SchemaFieldDescription(
        "Property used when scanning for data files to rewrite (default is false)")
    @Pure
    public abstract @Nullable Boolean getCaseSensitive();

    @Pure
    public abstract @Nullable String getFilter();

    @Pure
    public abstract @Nullable Map<String, String> getRewriteOptions();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSnapshotId(@Nullable Long snapshotId);

      public abstract Builder setCaseSensitive(@Nullable Boolean caseSensitive);

      public abstract Builder setFilter(@Nullable String filter);

      public abstract Builder setRewriteOptions(@Nullable Map<String, String> options);

      public abstract Configuration build();
    }
  }

  private static final TupleTag<SerializableDataFile> ADD_DATA_FILES = new TupleTag<>();
  private static final TupleTag<SerializableDataFile> DELETE_DATA_FILES = new TupleTag<>();

  static final SchemaCoder<RewriteFileGroup> FILE_GROUP_CODER;
  static SchemaCoder<SerializableDataFile> SERIALIZABLE_DF_CODER;
  static SchemaCoder<SnapshotInfo> SNAPSHOT_CODER;

  static {
    try {
      FILE_GROUP_CODER = SchemaRegistry.createDefault().getSchemaCoder(RewriteFileGroup.class);
      SERIALIZABLE_DF_CODER =
          SchemaRegistry.createDefault().getSchemaCoder(SerializableDataFile.class);
      SNAPSHOT_CODER = SchemaRegistry.createDefault().getSchemaCoder(SnapshotInfo.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }
}
