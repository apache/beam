package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.operator.WindowAware;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlowTranslator;
import cz.seznam.euphoria.flink.streaming.io.DataSinkWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.WindowingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamingFlowTranslator extends FlowTranslator {

  // static mapping of Euphoria operators to corresponding Flink transformations
  private static final Map<Class<? extends Operator<?, ?>>, StreamingOperatorTranslator> TRANSLATORS =
          new IdentityHashMap<>();

  static {
    // TODO add full support of all operators
    TRANSLATORS.put((Class) FlowUnfolder.InputOperator.class, new InputTranslator());
    TRANSLATORS.put((Class) FlatMap.class, new FlatMapTranslator());
    TRANSLATORS.put((Class) Repartition.class, new RepartitionTranslator());
    TRANSLATORS.put((Class) ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    TRANSLATORS.put((Class) Join.class, new JoinTranslator());
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

    // determine whether we'll be running with event or processing time characteristics
    assignTimeCharacteristic(env, dag);

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Operator<?, ?> originalOp = op.getOriginalOperator();
      StreamingOperatorTranslator translator = TRANSLATORS.get((Class) originalOp.getClass());
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

  private void assignTimeCharacteristic(
      StreamExecutionEnvironment env,
      DAG<FlinkOperator<?>> dag) {

    List<WindowingMode> modes = dag.traverse()
        .map(Node::get)
        .map(n -> n.getOriginalOperator())
        .filter(o -> o instanceof WindowAware)
        .map(o -> ((WindowAware) o).getWindowing())
        .filter(w -> w != null)
        .map(WindowingMode::determine)
        .distinct()
        .collect(Collectors.toList());
    switch (modes.size()) {
      case 0:
        // nothing to do
        break;
      case 1:
        env.setStreamTimeCharacteristic(modes.get(0).timeCharacteristic());
        break;
      default:
        throw new IllegalStateException(
            "Cannot mix different windowing modes in one flow!");
    }
  }

  @Override
  public Set<Class<? extends Operator<?, ?>>> getSupportedOperators() {
    return TRANSLATORS.keySet();
  }
}
