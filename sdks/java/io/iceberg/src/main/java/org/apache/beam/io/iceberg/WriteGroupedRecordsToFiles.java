package org.apache.beam.io.iceberg;

import org.apache.beam.io.iceberg.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

@SuppressWarnings("all")
public class WriteGroupedRecordsToFiles<DestinationT,ElementT>
    extends DoFn<KV<DestinationT,Iterable<ElementT>>, Result<DestinationT>> {

  private final PCollectionView<String> locationPrefixView;
  private final long maxFileSize;
  private final RecordWriterFactory<ElementT, DestinationT> recordWriterFactory;

  WriteGroupedRecordsToFiles(
      PCollectionView<String> locationPrefixView,
      long maxFileSize,
      RecordWriterFactory<ElementT, DestinationT> recordWriterFactory) {
    this.locationPrefixView = locationPrefixView;
    this.maxFileSize = maxFileSize;
    this.recordWriterFactory = recordWriterFactory;
  }

  @ProcessElement
  public void processElement(ProcessContext c,@Element KV<DestinationT,Iterable<ElementT>> element)
    throws Exception {
    String locationPrefix = c.sideInput(locationPrefixView);
    DestinationT destination = element.getKey();
    RecordWriter<ElementT> writer = recordWriterFactory.createWriter(locationPrefix,destination);
    for(ElementT e : element.getValue()) {
      writer.write(e);
    }
    writer.close();
    c.output(new Result<>(writer.table().name(),writer.location(), writer.dataFile(), writer.table().spec(), destination));
  }


}
