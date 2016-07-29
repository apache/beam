package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.flink.translation.FlowTranslator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Executor implementation using Apache Flink as a runtime
 */
public class FlinkExecutor implements Executor {

  private StreamExecutionEnvironment flinkStreamEnv;

  public FlinkExecutor() {
    this(StreamExecutionEnvironment.getExecutionEnvironment());
  }

  public FlinkExecutor(StreamExecutionEnvironment flinkStreamEnv) {
    this.flinkStreamEnv = flinkStreamEnv;
  }

  @Override
  public Future<Integer> submit(Flow flow) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int waitForCompletion(Flow flow) throws Exception {
    FlowTranslator translator = new FlowTranslator();
    List<DataSink<?>> sinks = translator.translateInto(flow, flinkStreamEnv);

    try {
      flinkStreamEnv.execute(); // blocking operation
    } catch (Exception e) {
      // when exception thrown rollback all sinks
      sinks.stream().forEach(DataSink::rollback);
      throw e;
    }

    // when the execution is successful commit all sinks
    for (DataSink s : sinks) {
      s.commit();
    }

    return 0;
  }
}
