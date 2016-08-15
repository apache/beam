package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.ExecutionEnvironment;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlowTranslator;
import cz.seznam.euphoria.flink.batch.io.DataSinkWrapper;
import org.apache.flink.api.java.DataSet;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BatchFlowTranslator extends FlowTranslator {

  // static mapping of Euphoria operators to corresponding Flink transformations
  private static final Map<Class<? extends Operator<?, ?>>, BatchOperatorTranslator> TRANSLATORS =
          new IdentityHashMap<>();

  static {
    // TODO add full support of all operators
    TRANSLATORS.put((Class) FlowUnfolder.InputOperator.class, new InputTranslator());
    TRANSLATORS.put((Class) FlatMap.class, new FlatMapTranslator());
    TRANSLATORS.put((Class) ReduceByKey.class, new ReduceByKeyTranslator());
  }


  @Override
  @SuppressWarnings("unchecked")
  public List<DataSink<?>> translateInto(Flow flow,
                                         ExecutionEnvironment executionEnvironment)
  {
    // transform flow to acyclic graph of supported operators
    DAG<FlinkOperator<?>> dag = flowToDag(flow);

    BatchExecutorContext executorContext =
            new BatchExecutorContext(executionEnvironment.getBatchEnv(), dag);

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Operator<?, ?> originalOp = op.getOriginalOperator();
      BatchOperatorTranslator translator = TRANSLATORS.get(originalOp.getClass());
      if (translator == null) {
        throw new UnsupportedOperationException(
                "Operator " + op.getClass().getSimpleName() + " not supported");
      }

      DataSet<?> out = translator.translate(op, executorContext);

      // save output of current operator to context
      executorContext.setOutput(op, out);
    });

    // process all sinks in the DAG (leaf nodes)
    final List<DataSink<?>> sinks = new ArrayList<>();
    dag.getLeafs()
            .stream()
            .map(Node::get)
            .filter(op -> op.output().getOutputSink() != null)
            .forEach(op -> {

              final DataSink<?> sink = op.output().getOutputSink();
              sinks.add(sink);
              DataSet<?> flinkOutput =
                      Objects.requireNonNull(executorContext.getOutputStream(op));

              flinkOutput.output(new DataSinkWrapper<>((DataSink) sink));
            });

    return sinks;
  }

  @Override
  public Set<Class<? extends Operator<?, ?>>> getSupportedOperators() {
    return TRANSLATORS.keySet();
  }
}
