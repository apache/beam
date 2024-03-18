package org.apache.beam.io.iceberg.util;

import org.apache.beam.io.iceberg.Iceberg;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

public class ScanHelper {

  public static boolean isIncremental(Iceberg.Scan scan) {
    if(scan.getFromSnapshotExclusive() != null) {
      return true;
    }
    return false;
  }

  public static TableScan tableScan(Table table,Iceberg.Scan scan) {
    TableScan tableScan = table.newScan();
    return tableScan;
  }


}
