package org.apache.beam.io.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.io.iceberg.util.SchemaHelper;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;

public class IcebergBoundedSource extends BoundedSource {

  private @Nullable CombinedScanTask task;
  private Iceberg.Scan scan;

  public IcebergBoundedSource(Iceberg.Scan scan, @Nullable CombinedScanTask task) {
    this.task = task;
    this.scan = scan;
  }

  public IcebergBoundedSource(Iceberg.Scan scan) {
    this(scan,null);
  }

  @Override
  public List<? extends BoundedSource> split(
     long desiredBundleSizeBytes,
     PipelineOptions options) throws Exception {
    ArrayList<IcebergBoundedSource> tasks = new ArrayList<>();
    Catalog catalog = scan.getCatalog().catalog();
    if(catalog != null) {
      Table table = catalog.loadTable(
          TableIdentifier.of(
              scan.getTable().toArray(new String[scan.getTable().size()]))
      );
      if(table != null) {
        switch(scan.getType()) {
          case TABLE:
            TableScan tableScan = table.newScan();

            if(scan.getProject() != null) {
              tableScan = tableScan.project(SchemaHelper.convert(scan.getProject()));
            }
            if(scan.getFilter() != null) {
              tableScan = tableScan.filter(scan.getFilter());
            }
            if(scan.getCaseSensitive() != null) {
              tableScan = tableScan.caseSensitive(scan.getCaseSensitive());
            }
            if(scan.getSnapshot() != null) {
              tableScan = tableScan.useSnapshot(scan.getSnapshot());
            }
            if(scan.getBranch() != null) {
              tableScan = tableScan.useRef(scan.getBranch());
            } else if(scan.getTag() != null) {
              tableScan = tableScan.useRef(scan.getTag());
            }




            try(CloseableIterable<CombinedScanTask> t = tableScan.planTasks()) {
              for(CombinedScanTask c : t) {
                tasks.add(new IcebergBoundedSource(scan,c));
              }
             }
            break;
          case BATCH:
            //TODO: Add batch scan
            break;
        }
      }
    }


    return tasks;
  }

  @Override
  public long getEstimatedSizeBytes(
      PipelineOptions options) throws Exception {
    return 0;
  }

  @Override
  public BoundedReader createReader(
      PipelineOptions options) throws IOException {
    return null;
  }
}
