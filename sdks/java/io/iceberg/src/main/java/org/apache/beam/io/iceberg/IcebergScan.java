package org.apache.beam.io.iceberg;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

@SuppressWarnings({"all"})
@AutoValue
@AutoValue.CopyAnnotations
public abstract class IcebergScan implements Serializable {

  enum ScanType {
    TABLE,
    BATCH,
    INCREMENTAL_APPEND,
    INCREMENTAL_CHANGELOG
  }

  public abstract String getCatalogName();
  public abstract ImmutableMap<String,String> getCatalogConfiguration();

  public abstract ImmutableMap<String,String> getHadoopConfiguration();

  public abstract ScanType getScanType();

  public abstract String getTable();

  public abstract ImmutableList<String> getColumns();

  public abstract boolean getCaseSensitive();

  public abstract ImmutableMap<String,String> getOptions();

  public abstract @Nullable Long getSnapshot();

  public abstract @Nullable Long getTimestamp();

  public abstract @Nullable Long getFromSnapshotInclusive();

  public abstract @Nullable String getFromSnapshotRefInclusive();

  public abstract @Nullable Long getFromSnapshotExclusive();

  public abstract @Nullable String getFromSnapshotRefExclusive();

  public abstract @Nullable Long getToSnapshot();

  public abstract @Nullable String getToSnapshotRef();

  public abstract @Nullable String getTag();

  public abstract @Nullable String getBranch();

  public static Builder builder() {
    return new AutoValue_IcebergScan.Builder()

            .catalogConfiguration(ImmutableMap.of())
            .hadoopConfiguration(ImmutableMap.of())
            .options(ImmutableMap.of())

            .columns(ImmutableList.of())
            .caseSensitive(true)

            .snapshot(null)
            .timestamp(null)
            .fromSnapshotInclusive(null)
            .fromSnapshotRefInclusive(null)
            .fromSnapshotExclusive(null)
            .fromSnapshotRefExclusive(null)

            .toSnapshot(null)
            .toSnapshotRef(null)

            .tag(null)
            .branch(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder catalogName(String catalogName);
    public abstract Builder catalogConfiguration(ImmutableMap<String,String> catalogConfiguration);
    public abstract Builder hadoopConfiguration(ImmutableMap<String,String> hadoopConfiguration);
    public abstract Builder scanType(ScanType scanType);
    public abstract Builder table(String string);

    public abstract Builder columns(ImmutableList<String> columns);

    public abstract Builder caseSensitive(boolean caseSensitive);

    public abstract Builder options(ImmutableMap<String,String> options);

    public abstract Builder snapshot(@Nullable Long snapshot);


    public abstract Builder timestamp(@Nullable Long timestamp);

    public abstract Builder fromSnapshotInclusive(@Nullable Long snapshotInclusive);
    public abstract Builder fromSnapshotRefInclusive(@Nullable String snapshotRefInclusive);

    public abstract Builder fromSnapshotExclusive(@Nullable Long snapshotExclusive);

    public abstract Builder fromSnapshotRefExclusive(@Nullable String snapshotRefExclusive);

    public abstract Builder toSnapshot(@Nullable Long toSnapshot);

    public abstract Builder toSnapshotRef(@Nullable String toRef);

    public abstract Builder tag(@Nullable String tag);

    public abstract Builder branch(@Nullable String branch);

    public abstract IcebergScan build();
  }

  @Nullable
  private transient Configuration hadoopConf;

  public Configuration hadoopConf() {
    if(hadoopConf == null) {
      Configuration local = new Configuration();
      getHadoopConfiguration().forEach((k,v) -> {
        local.set(k,v);
      });
      hadoopConf = local;
      return local;
    } else {
      return hadoopConf;
    }
  }

  @Nullable
  private transient Catalog catalog;

  public Catalog catalog() {
    if(catalog == null) {
      catalog = CatalogUtil.buildIcebergCatalog(getCatalogName(),
          getCatalogConfiguration(),hadoopConf());
    }
    return catalog;
  }

  @Nullable
  private transient Table table;

  public Table table() {
    if(table == null) {
      Table local = catalog().loadTable(TableIdentifier.parse(getTable()));
      table = local;
      return local;
    } else {
      return table;
    }
  }


  public TableScan tableScan() {
    TableScan s = table().newScan();
    s = s.caseSensitive(getCaseSensitive());
    for(Entry<String,String> e : getOptions().entrySet()) {
      s = s.option(e.getKey(),e.getValue());
    }
    if(getColumns().size() > 0) {
      s = s.select(getColumns());
    }
    if(getSnapshot() != null) {
      s = s.useSnapshot(getSnapshot());
    }
    if(getTag() != null) {
      s = s.useRef(getTag());
    }
    if(getBranch() != null) {
      s = s.useRef(getBranch());
    }
    if(getTimestamp() != null) {
      s = s.asOfTime(getTimestamp());
    }

    return s;
  }

  public BatchScan batchScan() {

    BatchScan s = table().newBatchScan();
    for(Entry<String,String> e : getOptions().entrySet()) {
      s = s.option(e.getKey(),e.getValue());
    }
    if(getColumns().size() > 0) {
      s = s.select(getColumns());
    }
    if(getSnapshot() != null) {
      s = s.useSnapshot(getSnapshot());
    }
    if(getTag() != null) {
      s = s.useRef(getTag());
    }
    if(getBranch() != null) {
      s = s.useRef(getBranch());
    }
    if(getTimestamp() != null) {
      s = s.asOfTime(getTimestamp());
    }
    return s;
  }

  public IncrementalAppendScan appendScan() {
    IncrementalAppendScan s = table().newIncrementalAppendScan();
    for(Entry<String,String> e : getOptions().entrySet()) {
      s = s.option(e.getKey(),e.getValue());
    }
    if(getColumns().size() > 0) {
      s = s.select(getColumns());
    }
    if(getFromSnapshotInclusive() != null) {
      s = s.fromSnapshotInclusive(getFromSnapshotInclusive());
    }
    if(getFromSnapshotRefInclusive() != null) {
      s = s.fromSnapshotInclusive(getFromSnapshotRefInclusive());
    }
    if(getFromSnapshotExclusive() != null) {
      s = s.fromSnapshotExclusive(getFromSnapshotRefExclusive());
    }
    if(getFromSnapshotRefExclusive() != null) {
      s = s.fromSnapshotExclusive(getFromSnapshotExclusive());
    }

    if(getToSnapshot() != null) {
      s = s.toSnapshot(getToSnapshot());
    }
    if(getToSnapshotRef() != null) {
      s = s.toSnapshot(getToSnapshotRef());
    }

    return s;
  }

  public IncrementalChangelogScan changelogScan() {
    IncrementalChangelogScan s = table().newIncrementalChangelogScan();
    s = s.caseSensitive(getCaseSensitive());
    for(Entry<String,String> e : getOptions().entrySet()) {
      s = s.option(e.getKey(),e.getValue());
    }
    if(getFromSnapshotInclusive() != null) {
      s = s.fromSnapshotInclusive(getFromSnapshotInclusive());
    }
    if(getFromSnapshotRefInclusive() != null) {
      s = s.fromSnapshotInclusive(getFromSnapshotRefInclusive());
    }
    if(getFromSnapshotExclusive() != null) {
      s = s.fromSnapshotExclusive(getFromSnapshotRefExclusive());
    }
    if(getFromSnapshotRefExclusive() != null) {
      s = s.fromSnapshotExclusive(getFromSnapshotExclusive());
    }
    if(getToSnapshot() != null) {
      s = s.toSnapshot(getToSnapshot());
    }
    if(getToSnapshotRef() != null) {
      s = s.toSnapshot(getToSnapshotRef());
    }
    return s;
  }


}
