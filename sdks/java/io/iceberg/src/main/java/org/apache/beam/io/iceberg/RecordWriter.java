package org.apache.beam.io.iceberg;

import java.io.IOException;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

@SuppressWarnings("all") //TODO: Remove this once development is stable.
class RecordWriter<ElementT> {

  final Table table;

  final DataWriter<Record> writer;
  final GenericRecord baseRecord;
  final SerializableBiFunction<Record,ElementT,Record> toRecord;

  final String location;

  RecordWriter(
      Table table,
      String location,
      Schema schema,
      PartitionSpec partitionSpec,
      FileFormat format,
      SerializableBiFunction<Record,ElementT,Record> toRecord
      ) throws IOException {
    this.table = table;
    this.baseRecord = GenericRecord.create(schema);
    this.toRecord = toRecord;
    this.location = table.locationProvider().newDataLocation(partitionSpec,baseRecord,location);

    OutputFile outputFile = table.io().newOutputFile(this.location);
    switch (format) {
      case AVRO:
        writer = Avro.writeData(outputFile)
            .schema(schema)
            .withSpec(partitionSpec)
            .overwrite().build();
        break;
      case PARQUET:
        writer = Parquet.writeData(outputFile)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .schema(schema)
            .withSpec(partitionSpec)
            .overwrite().build();
        break;
      case ORC:
        writer = ORC.writeData(outputFile)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(schema)
            .withSpec(partitionSpec)
            .overwrite().build();
        break;
      default:
        throw new RuntimeException("Unrecognized File Format. This should be impossible.");
    }
  }

  public void write(ElementT element) throws IOException {
    Record record = toRecord.apply(baseRecord,element);
    writer.write(record);
  }

  public void close() throws IOException {
    writer.close();
  }

  public long bytesWritten() {
    return writer.length();
  }

  public Table table() { return table; }

  public String location() {
    return location;
  }

  public DataFile dataFile() {
    return writer.toDataFile();
  }


}
