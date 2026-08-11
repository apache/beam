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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A lightweight adapter that implements {@link Table} backed by a {@link SerializableTableSpec}.
 *
 * <p>Delegates declarative metadata (schema, partition specs, sort order, properties) to the
 * broadcasted {@link SerializableTableSpec} and file I/O to a worker-local {@link FileIO} instance.
 *
 * <p>Mutation operations (e.g. {@code newAppend()}, {@code updateSchema()}) throw {@link
 * UnsupportedOperationException} because table commits are handled centrally in {@link
 * AppendFilesToTables}.
 */
@Internal
@SuppressWarnings("nullness")
public class SideInputTable implements Table {

  private final SerializableTableSpec spec;
  private final FileIO fileIO;
  private final EncryptionManager encryptionManager;
  private final LocationProvider locationProvider;

  public SideInputTable(SerializableTableSpec spec, FileIO fileIO) {
    this(spec, fileIO, PlaintextEncryptionManager.instance());
  }

  public SideInputTable(
      SerializableTableSpec spec, FileIO fileIO, EncryptionManager encryptionManager) {
    this.spec = checkNotNull(spec, "spec must not be null");
    this.fileIO = checkNotNull(fileIO, "fileIO must not be null");
    this.encryptionManager = checkNotNull(encryptionManager, "encryptionManager must not be null");
    this.locationProvider =
        LocationProviders.locationsFor(spec.getLocation(), spec.getProperties());
  }

  public SerializableTableSpec getTableSpec() {
    return spec;
  }

  @Override
  public String name() {
    return spec.getName();
  }

  @Override
  public String location() {
    return spec.getLocation();
  }

  @Override
  public Schema schema() {
    return spec.getSchema();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return Collections.singletonMap(spec.getSchema().schemaId(), spec.getSchema());
  }

  @Override
  public PartitionSpec spec() {
    return spec.getPartitionSpec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return Collections.singletonMap(spec.getPartitionSpec().specId(), spec.getPartitionSpec());
  }

  @Override
  public SortOrder sortOrder() {
    return spec.getSortOrder();
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return Collections.singletonMap(spec.getSortOrder().orderId(), spec.getSortOrder());
  }

  @Override
  public Map<String, String> properties() {
    return spec.getProperties();
  }

  @Override
  public LocationProvider locationProvider() {
    return locationProvider;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  public EncryptionManager encryption() {
    return encryptionManager;
  }

  @Override
  public void refresh() {
    // No-op: refresh is managed by the periodic side-input update mechanism
  }

  @Override
  public @Nullable Snapshot currentSnapshot() {
    return null;
  }

  @Override
  public @Nullable Snapshot snapshot(long snapshotId) {
    return null;
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return Collections.emptyList();
  }

  @Override
  public List<HistoryEntry> history() {
    return Collections.emptyList();
  }

  @Override
  public Map<String, SnapshotRef> refs() {
    return Collections.emptyMap();
  }

  @Override
  public List<StatisticsFile> statisticsFiles() {
    return Collections.emptyList();
  }

  @Override
  public List<PartitionStatisticsFile> partitionStatisticsFiles() {
    return Collections.emptyList();
  }

  @Override
  public TableScan newScan() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support scans directly.");
  }

  @Override
  public IncrementalAppendScan newIncrementalAppendScan() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support scans directly.");
  }

  @Override
  public IncrementalChangelogScan newIncrementalChangelogScan() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support scans directly.");
  }

  @Override
  public UpdateSchema updateSchema() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public UpdateProperties updateProperties() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public AppendFiles newAppend() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public AppendFiles newFastAppend() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public OverwriteFiles newOverwrite() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public RowDelta newRowDelta() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public DeleteFiles newDelete() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public UpdateStatistics updateStatistics() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public Transaction newTransaction() {
    throw new UnsupportedOperationException(
        "SideInputTable is a read-only metadata adapter and does not support table mutations.");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SideInputTable)) {
      return false;
    }
    SideInputTable that = (SideInputTable) o;
    return Objects.equals(spec, that.spec)
        && Objects.equals(fileIO, that.fileIO)
        && Objects.equals(encryptionManager, that.encryptionManager);
  }

  @Override
  public int hashCode() {
    return Objects.hash(spec, fileIO, encryptionManager);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("spec", spec)
        .add("fileIO", fileIO)
        .add("encryptionManager", encryptionManager)
        .toString();
  }
}
