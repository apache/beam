package org.apache.beam.io.iceberg;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

public class MetadataUpdates<IdentifierT> extends DoFn<KV<IdentifierT,Iterable<MetadataUpdate>>,KV<IdentifierT,Snapshot>> {

  final TableFactory<IdentifierT> tableFactory;

  public MetadataUpdates(TableFactory<IdentifierT> tableFactory) {
    this.tableFactory = tableFactory;
  }

  @ProcessElement
  public void processElement(ProcessContext c,@Element KV<IdentifierT,Iterable<MetadataUpdate>> element,
      BoundedWindow window) {
    Table table = tableFactory.getTable(element.getKey());
    AppendFiles update = table.newAppend();
    Iterable<MetadataUpdate> metadataUpdates = element.getValue();
    if(metadataUpdates != null) {
      for(MetadataUpdate metadata : metadataUpdates) {
        for(DataFile file : metadata.getDataFiles()) {
          update.appendFile(file);
        }
      }
      update.commit();
      c.outputWithTimestamp(KV.of(element.getKey(),table.currentSnapshot()),window.maxTimestamp());
    }
  }

}
