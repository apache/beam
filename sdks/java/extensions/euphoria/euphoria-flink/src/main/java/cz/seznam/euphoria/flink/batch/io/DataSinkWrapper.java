package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class DataSinkWrapper<T> implements OutputFormat<T> {

  private final DataSink<T> dataSink;

  private transient Writer<T> writer;

  public DataSinkWrapper(DataSink<T> dataSink) {
    this.dataSink = dataSink;
  }

  @Override
  public void configure(Configuration parameters) {
    // ignore configuration
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    writer = dataSink.openWriter(taskNumber);
  }

  @Override
  public void writeRecord(T record) throws IOException {
    writer.write(record);
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.commit();
      writer.close();
    }
  }
}
