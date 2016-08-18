package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.executor.FlowUnfolder;

import java.util.List;
import java.util.Set;

/**
 * Translates given {@link Flow} into Flink execution environment
 */
public abstract class FlowTranslator {

  /**
   * Translates given flow to Flink execution environment
   * @return List of {@link DataSink} processed in given flow (leaf nodes)
   */
  @SuppressWarnings("unchecked")
  public abstract List<DataSink<?>> translateInto(Flow flow);

  /**
   * Converts {@link Flow} to {@link DAG} of Flink specific {@link FlinkOperator}
   */
  protected DAG<FlinkOperator<?>> flowToDag(Flow flow) {
    DAG<Operator<?, ?>> unfolded = FlowUnfolder.unfold(flow, getSupportedOperators());
    DAG<FlinkOperator<?>> dag = createOptimizer().optimize(unfolded);

    return dag;
  }

  protected FlowOptimizer createOptimizer() {
    return new FlowOptimizer();
  }

  public abstract Set<Class<? extends Operator<?, ?>>> getSupportedOperators();
}
