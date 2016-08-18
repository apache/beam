package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlowTranslator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StreamingFlowTranslator extends FlowTranslator {

  // static mapping of Euphoria operators to corresponding Flink transformations
  private static final Map<Class<? extends Operator<?, ?>>, StreamingOperatorTranslator> TRANSLATORS =
          new IdentityHashMap<>();

  static {
    // TODO add full support of all operators
    TRANSLATORS.put((Class) FlowUnfolder.InputOperator.class, new InputTranslator());
    TRANSLATORS.put((Class) FlatMap.class, new FlatMapTranslator());
    TRANSLATORS.put((Class) Repartition.class, new RepartitionTranslator());
    //TRANSLATORS.put((Class) ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    TRANSLATORS.put((Class) Union.class, new UnionTranslator());

    TRANSLATORS.put((Class) ReduceByKey.class, new ReduceByKeyTranslator());
  }

  private final StreamExecutionEnvironment env;

  public StreamingFlowTranslator(StreamExecutionEnvironment env) {
    this.env = Objects.requireNonNull(env);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<DataSink<?>> translateInto(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<FlinkOperator<?>> dag = flowToDag(flow);

    StreamingExecutorContext executorContext = new StreamingExecutorContext(env, dag);

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Operator<?, ?> originalOp = op.getOriginalOperator();
      StreamingOperatorTranslator translator = TRANSLATORS.get(originalOp.getClass());
      if (translator == null) {
        throw new UnsupportedOperationException(
                "Operator " + op.getClass().getSimpleName() + " not supported");
      }

      DataStream<?> out = translator.translate(op, executorContext);

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
              DataStream<?> flinkOutput =
                      Objects.requireNonNull(executorContext.getOutputStream(op));

              flinkOutput.addSink(new DataSinkWrapper<>((DataSink) sink))
                      .setParallelism(op.getParallelism());
            });

    return sinks;
  }

  @Override
  public Set<Class<? extends Operator<?, ?>>> getSupportedOperators() {
    return TRANSLATORS.keySet();
  }
}
