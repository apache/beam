package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import java.io.IOException;

public interface BigQueryStorageReader extends AutoCloseable {

  void processReadRowsResponse(ReadRowsResponse readRowsResponse) throws IOException;

  long getRowCount();

  Object readSingleRecord() throws IOException;

  boolean readyForNextReadResponse() throws IOException;

  void resetBuffer();

  @Override
  void close();
}
