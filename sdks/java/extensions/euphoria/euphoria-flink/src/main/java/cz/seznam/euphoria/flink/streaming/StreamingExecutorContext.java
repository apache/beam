package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.flink.ExecutorContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamingExecutorContext extends ExecutorContext<DataStream<?>> {

  private final StreamExecutionEnvironment executionEnvironment;

  public StreamingExecutorContext(StreamExecutionEnvironment executionEnvironment,
                                  DAG<FlinkOperator<?>> dag)
  {
    super(dag);
    this.executionEnvironment = executionEnvironment;
  }

  public StreamExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }
}
