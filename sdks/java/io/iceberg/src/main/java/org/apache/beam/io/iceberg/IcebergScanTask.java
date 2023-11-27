package org.apache.beam.io.iceberg;

import java.io.Serializable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTaskParser;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;

public class IcebergScanTask implements Serializable {


  private CombinedScanTask task;
  private FileIO io;
  private EncryptionManager encryption;

  public CombinedScanTask task() { return task; }

  public FileIO io() { return io; }

  public EncryptionManager encryption() { return encryption; }

  public IcebergScanTask(CombinedScanTask task,FileIO io,EncryptionManager encryption) {
    this.task = task;
    this.io = io;
    this.encryption = encryption;
  }

}
