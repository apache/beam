package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.flink.ExecutorContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class BatchExecutorContext
    extends ExecutorContext<ExecutionEnvironment, DataSet<?>>
{
  public BatchExecutorContext(ExecutionEnvironment env, DAG<FlinkOperator<?>> dag) {
    super(env, dag);
  }
}
