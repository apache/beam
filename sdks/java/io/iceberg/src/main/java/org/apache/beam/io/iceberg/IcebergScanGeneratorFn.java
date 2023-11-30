package org.apache.beam.io.iceberg;

import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates work from a scan description. Corresponds to the InputSplit planning phase
 * of other systems (e.g. Flink) which execute the equivalent code on the JobManager. Since
 * Beam runners are all identical we treat that phase as essentially an Impulse operation.
 *
 * In theory this allows a Beam pipeline to operate over more than one catalog.
 */
public class IcebergScanGeneratorFn extends DoFn<IcebergScan,IcebergScanTask> {
  Logger LOG = LoggerFactory.getLogger(IcebergScanGeneratorFn.class);

  private void processTableScan(TableScan scan,FileIO io,EncryptionManager encryptionManager, ProcessContext c) {
    LOG.info("Starting a table scan with table {}",scan.table().name());
    int counter = 0;
    try(CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      for(CombinedScanTask task : tasks) {
        c.output(new IcebergScanTask(task,io,encryptionManager));
        counter++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Produced {} scan tasks from table scan",counter);
  }

  private void processBatchScan(BatchScan scan,ProcessContext c) {
    //TODO: Decide if this is even necessary to implement.
    LOG.info("Starting a batch scan for table {}",scan.table().name());
  }

  private void processAppendScan(IncrementalAppendScan scan,FileIO io,EncryptionManager encryptionManager, ProcessContext c) {
    LOG.info("Starting an incremental append scan");
    int counter = 0;
    try(CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      for(CombinedScanTask task : tasks) {
        c.output(new IcebergScanTask(task,io,encryptionManager));
        counter++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Produced {} scan tasks from incremental table scan",counter);
  }

  private void processChangelogScan(IncrementalChangelogScan scan,ProcessContext c) {
    //TODO: Changelog scans operate differently than table or incremental append scans.
    LOG.info("Starting a changelog scan");
  }

  @ProcessElement
  public void process(ProcessContext c) {
    Table t = c.element().table();
    FileIO io = t.io();
    EncryptionManager encryptionManager = t.encryption();

    switch(c.element().getScanType()) {

      case TABLE:
        processTableScan(c.element().tableScan(),io,encryptionManager,c);;
        break;
      case BATCH:
        break;
      case INCREMENTAL_APPEND:
        processAppendScan(c.element().appendScan(),io,encryptionManager,c);
        break;
      case INCREMENTAL_CHANGELOG:
        processChangelogScan(c.element().changelogScan(),c);
        break;
    }

  }

}
