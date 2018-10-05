package cz.seznam.euphoria.flink.streaming.io;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;

public class DataSinkWrapper<T>
    extends RichSinkFunction<StreamingWindowedElement<?, T>>
    implements Checkpointed {

  private DataSink<T> dataSink;
  private Writer<T> writer;

  public DataSinkWrapper(DataSink<T> dataSink) {
    this.dataSink = dataSink;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    dataSink.initialize();

    RuntimeContext ctx = getRuntimeContext();
    final int subtaskId = ctx.getIndexOfThisSubtask();

    writer = dataSink.openWriter(subtaskId);
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.flush();
      writer.commit();
      writer.close();
    }
  }

  @Override
  public void invoke(StreamingWindowedElement<?, T> elem) throws Exception {
    writer.write(elem.get());
  }

  @Override
  public Serializable snapshotState(long l, long l1) throws Exception {
    writer.flush();
    return dataSink;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void restoreState(Serializable t) throws Exception {
    this.dataSink = (DataSink) t;
  }
}
