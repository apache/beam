package org.apache.beam.io.iceberg;

import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

@SuppressWarnings("all") //TODO: Remove this once development is stable.
abstract class RecordWriterFactory<ElementT, DestinationT> implements Serializable {
  private RecordWriterFactory() { }

  public abstract RecordWriterFactory<ElementT,DestinationT> prepare(DynamicDestinations<?,DestinationT> destination);

  public abstract RecordWriter<ElementT> createWriter(String location,DestinationT destination) throws Exception;

  static <ElementT,DestinationT> TableRecordWriterFactory<ElementT,DestinationT> tableRecords(
      SerializableBiFunction<Record,ElementT, Record> toRecord,
      @Nullable DynamicDestinations<?,DestinationT> dynamicDestinations
      ) {
    return new TableRecordWriterFactory<>(toRecord,dynamicDestinations);
  }


  static final class TableRecordWriterFactory<ElementT,DestinationT> extends RecordWriterFactory<ElementT,DestinationT>  {

    final SerializableBiFunction<Record,ElementT,Record> toRecord;

    final DynamicDestinations<?,DestinationT> dynamicDestinations;

    TableRecordWriterFactory(
        SerializableBiFunction<Record,ElementT,Record> toRecord,
        DynamicDestinations<?,DestinationT> dynamicDestinations) {
      this.toRecord = toRecord;
      this.dynamicDestinations = dynamicDestinations;
    }


    @Override
    public RecordWriterFactory<ElementT, DestinationT> prepare(DynamicDestinations<?,DestinationT> destination) {
      return new TableRecordWriterFactory<>(toRecord,destination);
    }

    @Override
    public RecordWriter<ElementT> createWriter(String location,DestinationT destination)
        throws Exception {
      Table table = dynamicDestinations.getTable(destination);
      Schema schema = dynamicDestinations.getSchema(destination);
      PartitionSpec partitionSpec = dynamicDestinations.getPartitionSpec(destination);
      FileFormat format = dynamicDestinations.getFileFormat(destination);
      return new RecordWriter<>(
          table,location,schema,partitionSpec,format,toRecord);
    }
  }



}
