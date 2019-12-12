package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import java.io.IOException;

public class BigQueryStorageReaderFactory {

  private BigQueryStorageReaderFactory() { }

  public static BigQueryStorageReader getReader(ReadSession readSession) throws IOException {
    if (readSession.hasAvroSchema()) {
      return new BigQueryStorageAvroReader(readSession);
    } else if (readSession.hasArrowSchema()) {
      return new BigQueryStorageArrowReader(readSession);
    }
    throw new IllegalStateException("Read session does not have Avro/Arrow schema set.");
  }
}
