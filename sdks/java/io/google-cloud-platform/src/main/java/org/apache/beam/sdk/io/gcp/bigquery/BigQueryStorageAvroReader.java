package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroRows;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

public class BigQueryStorageAvroReader implements BigQueryStorageReader {

  private final Schema avroSchema;
  private final DatumReader<GenericRecord> datumReader;
  private BinaryDecoder decoder;
  private GenericRecord record;
  private long rowCount;

  public BigQueryStorageAvroReader(ReadSession readSession) {
    this.avroSchema = new Schema.Parser().parse(readSession.getAvroSchema().getSchema());
    this.datumReader = new GenericDatumReader<>(avroSchema);
    this.rowCount = 0;
  }

  @Override
  public void processReadRowsResponse(ReadRowsResponse readRowsResponse) {
    AvroRows avroRows = readRowsResponse.getAvroRows();
    rowCount = avroRows.getRowCount();
    decoder = DecoderFactory.get()
        .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public Object readSingleRecord() throws IOException {
    record = datumReader.read(record, decoder);
    return record;
  }

  @Override
  public boolean readyForNextReadResponse() throws IOException {
    return decoder == null || decoder.isEnd();
  }

  @Override
  public void close() { }
}
