package org.apache.beam.io.iceberg;

import java.io.Serializable;
import org.apache.iceberg.CombinedScanTask;

public class IcebergScanTask implements Serializable {

  private CombinedScanTask task;

  public CombinedScanTask task() { return task; }

  public IcebergScanTask(CombinedScanTask task) {
    this.task = task;
  }

}
