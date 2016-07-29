package cz.seznam.euphoria.flink.translation.io;

import cz.seznam.euphoria.core.client.io.DataSource;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class DataSourceWrapper<T> extends RichParallelSourceFunction<T> {

  private final DataSource<T> dataSource;

  public DataSourceWrapper(DataSource<T> dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {

  }

  @Override
  public void cancel() {

  }
}
