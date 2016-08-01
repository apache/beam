package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.flink.translation.FlowTranslator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
    translator.translateInto(flow, flinkStreamEnv);

    flinkStreamEnv.execute();

    return 0;
  }
}
