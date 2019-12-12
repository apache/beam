package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.ArrowProto;
import com.google.cloud.bigquery.storage.v1beta1.ArrowProto.ArrowSchema;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.sdk.schemas.utils.ArrowUtils;
import org.apache.beam.sdk.values.Row;

public class BigQueryStorageArrowReader implements BigQueryStorageReader {

  private final RootAllocator alloc;
  private final Schema arrowSchema;
  private final org.apache.beam.sdk.schemas.Schema arrowBeamSchema;
  private final VectorSchemaRoot vectorRoot;
  private final VectorLoader vectorLoader;
  private Iterator<Row> recordBatchIterable;
  private long rowCount;

  public BigQueryStorageArrowReader(ReadSession readSession) throws IOException {
    ArrowSchema protoSchema = readSession.getArrowSchema();
    ReadChannel readChannel = new ReadChannel(Channels.newChannel(protoSchema.getSerializedSchema().newInput()));

    this.alloc = new RootAllocator(Long.MAX_VALUE);
    this.arrowSchema = MessageSerializer.deserializeSchema(readChannel);
    this.arrowBeamSchema = ArrowUtils.toBeamSchema(arrowSchema);
    this.vectorRoot = VectorSchemaRoot.create(arrowSchema, alloc);
    this.vectorLoader = new VectorLoader(vectorRoot);
    this.rowCount = 0;
  }

  @Override
  public void processReadRowsResponse(ReadRowsResponse readRowsResponse) throws IOException {
    ArrowProto.ArrowRecordBatch recordBatch = readRowsResponse.getArrowRecordBatch();
    InputStream buffer = recordBatch.getSerializedRecordBatch().newInput();
    rowCount = recordBatch.getRowCount();

    vectorRoot.clear();
    ReadChannel readChannel = new ReadChannel(Channels.newChannel(buffer));
    ArrowRecordBatch arrowMessage = MessageSerializer.deserializeRecordBatch(readChannel, alloc);
    vectorLoader.load(arrowMessage);
    recordBatchIterable = new ArrowUtils.RecordBatchIterable(arrowBeamSchema, vectorRoot).iterator();
    arrowMessage.close();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public Object readSingleRecord() throws IOException {
    return recordBatchIterable.next();
  }

  @Override
  public boolean readyForNextReadResponse() throws IOException {
    return recordBatchIterable == null || !recordBatchIterable.hasNext();
  }

  @Override
  public void resetBuffer() {
    recordBatchIterable = null;
  }

  @Override
  public void close() {
    vectorRoot.close();
    alloc.close();
  }
}
