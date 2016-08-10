package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.flink.ExecutorContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class BatchExecutorContext extends ExecutorContext<DataSet<?>> {

  private final ExecutionEnvironment executionEnvironment;

  public BatchExecutorContext(ExecutionEnvironment executionEnvironment,
                              DAG<FlinkOperator<?>> dag)
  {
    super(dag);
    this.executionEnvironment = executionEnvironment;
  }

  public ExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }
}
