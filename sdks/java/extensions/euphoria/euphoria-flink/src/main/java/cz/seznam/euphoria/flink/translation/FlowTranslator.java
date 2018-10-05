package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

public class FlowTranslator {

  // static mapping of Euphoria operators to corresponding Flink transformations
  private static final Map<Class<? extends Operator<?, ?>>, OperatorTranslator> TRANSLATORS =
          new IdentityHashMap<>();

  static {
    // TODO add full support of all operators
    TRANSLATORS.put((Class) FlowUnfolder.InputOperator.class, new InputTranslator());
    TRANSLATORS.put((Class) FlatMap.class, new FlatMapTranslator());
    TRANSLATORS.put((Class) Repartition.class, new RepartitionTranslator());
    //TRANSLATORS.put((Class) ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    TRANSLATORS.put((Class) Union.class, new UnionTranslator());
  }

  private final StreamExecutionEnvironment streamExecutionEnvironment;
  private final TranslationContext translationContext;

  public FlowTranslator(StreamExecutionEnvironment streamExecutionEnvironment) {
    this.streamExecutionEnvironment = streamExecutionEnvironment;
    this.translationContext = new TranslationContext(streamExecutionEnvironment);
  }

  /**
   * Translates given flow to Flink specific API
   */
  @SuppressWarnings("unchecked")
  public void translate(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<Operator<?, ?>> dag = FlowUnfolder.unfold(flow, TRANSLATORS.keySet());

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      OperatorTranslator translator = TRANSLATORS.get(op.getClass());
      if (translator == null) {
        throw new UnsupportedOperationException(
                "Operator " + op.getClass().getSimpleName() + "not supported");
      }

      DataStream<?> out = translator.translate(op, translationContext);

      // save output of current operator to context
      translationContext.setOutputStream(op, out);
    });

    // process all sinks in the DAG (leaf nodes)
    dag.getLeafs().stream().map(Node::get).forEach(op -> {
      DataStream<?> flinkOutput =
              Objects.requireNonNull(translationContext.getOutputStream(op));


      // TODO sink wrapper
      flinkOutput.print();
    });
  }
}
