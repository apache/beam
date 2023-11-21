package org.apache.beam.io.iceberg;

import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.io.CloseableIterable;

public class IcebergScanGeneratorFn extends DoFn<IcebergScan,IcebergScanTask> {

  @ProcessElement
  public void process(ProcessContext c) {
    try(CloseableIterable<CombinedScanTask> tasks = c.element().scan().planTasks()) {
      for(CombinedScanTask task : tasks) {
        c.output(new IcebergScanTask(task));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
