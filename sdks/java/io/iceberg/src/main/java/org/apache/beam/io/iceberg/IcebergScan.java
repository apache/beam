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

  public abstract Optional<Long> getSnapshot();

  public abstract Optional<Long> getTimestamp();

  public abstract Optional<Long> getFromSnapshotInclusive();

  public abstract Optional<String> getFromSnapshotRefInclusive();

  public abstract Optional<Long> getFromSnapshotExclusive();

  public abstract Optional<String> getFromSnapshotRefExclusive();

  public abstract Optional<String> getTag();

  public abstract Optional<String> getBranch();

  @AutoValue.Builder
  public static abstract class Builder {

    public abstract Builder catalogName(String catalog);
    public abstract Builder catalogConfiguration(ImmutableMap<String,String> config);
    public abstract Builder hadoopConfiguration(ImmutableMap<String,String> config);

    public abstract IcebergScan build();
  }


  @Nullable
  private transient Configuration hadoopConf;

  public Configuration hadoopConf() {
    if(hadoopConf == null) {
      hadoopConf = new Configuration();
      getHadoopConfiguration().forEach((k,v) -> {
        hadoopConf.set(k,v);
      });
    }
    return hadoopConf;
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
      catalog().loadTable(TableIdentifier.parse(getTable()));
    }
    return table;
  }

  @Nullable
  private transient Scan scan;

  public TableScan tableScan() {
    TableScan s = table.newScan();
    s = s.caseSensitive(getCaseSensitive());
    for(Entry<String,String> e : getOptions().entrySet()) {
      s = s.option(e.getKey(),e.getValue());
    }
    if(getColumns().size() > 0) {
      s = s.select(getColumns());
    }
    if(getSnapshot().isPresent()) {
      s = s.useSnapshot(getSnapshot().get());
    }
    if(getTag().isPresent()) {
      s = s.useRef(getTag().get());
    }
    if(getBranch().isPresent()) {
      s = s.useRef(getBranch().get());
    }
    if(getTimestamp().isPresent()) {
      s = s.asOfTime(getTimestamp().get());
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
    if(getSnapshot().isPresent()) {
      s = s.useSnapshot(getSnapshot().get());
    }
    if(getTag().isPresent()) {
      s = s.useRef(getTag().get());
    }
    if(getBranch().isPresent()) {
      s = s.useRef(getBranch().get());
    }
    if(getTimestamp().isPresent()) {
      s = s.asOfTime(getTimestamp().get());
    }
    return s;
  }

  public IncrementalAppendScan appendScan() {
    IncrementalAppendScan s = table.newIncrementalAppendScan();
    for(Entry<String,String> e : getOptions().entrySet()) {
      s = s.option(e.getKey(),e.getValue());
    }
    if(getColumns().size() > 0) {
      s = s.select(getColumns());
    }
    if(getFromSnapshotInclusive().isPresent()) {
      s = s.fromSnapshotInclusive(getFromSnapshotInclusive().get());
    }
    if(getFromSnapshotRefInclusive().isPresent()) {
      s = s.fromSnapshotInclusive(getFromSnapshotRefInclusive().get());
    }
    if(getFromSnapshotExclusive().isPresent()) {
      s = s.fromSnapshotExclusive(getFromSnapshotRefExclusive().get());
    }
    if(getFromSnapshotRefExclusive().isPresent()) {
      s = s.fromSnapshotExclusive(getFromSnapshotExclusive().get());
    }
    return s;
  }

  public IncrementalChangelogScan changelogScan() {
    IncrementalChangelogScan s = table.newIncrementalChangelogScan();
    s = s.caseSensitive(getCaseSensitive());
    for(Entry<String,String> e : getOptions().entrySet()) {
      s = s.option(e.getKey(),e.getValue());
    }
    if(getFromSnapshotInclusive().isPresent()) {
      s = s.fromSnapshotInclusive(getFromSnapshotInclusive().get());
    }
    if(getFromSnapshotRefInclusive().isPresent()) {
      s = s.fromSnapshotInclusive(getFromSnapshotRefInclusive().get());
    }
    if(getFromSnapshotExclusive().isPresent()) {
      s = s.fromSnapshotExclusive(getFromSnapshotRefExclusive().get());
    }
    if(getFromSnapshotRefExclusive().isPresent()) {
      s = s.fromSnapshotExclusive(getFromSnapshotExclusive().get());
    }
    return s;
  }

  public Scan scan() {
    if(scan == null) {
      switch(getScanType()) {
        case TABLE:
          scan = tableScan();
          break;
        case BATCH:
          scan = batchScan();
          break;
        case INCREMENTAL_APPEND:
          scan = appendScan();
          break;
        case INCREMENTAL_CHANGELOG:
          scan = changelogScan();
          break;
      }
    }
    return scan;
  }



}
