package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import cz.seznam.euphoria.hadoop.output.DataSinkOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Translates given {@link Flow} into Spark execution environment
 */
public class SparkFlowTranslator {

  private static class Translation<O extends Operator<?, ?>> {
    final SparkOperatorTranslator<O> translator;
    final UnaryPredicate<O> accept;

    private Translation(
            SparkOperatorTranslator<O> translator, UnaryPredicate<O> accept) {
      this.translator = Objects.requireNonNull(translator);
      this.accept = accept;
    }

    static <O extends Operator<?, ?>> void set(
            Map<Class, Translation> idx,
            Class<O> type, SparkOperatorTranslator<O> translator)
    {
      set(idx, type, translator, null);
    }

    static <O extends Operator<?, ?>> void set(
            Map<Class, Translation> idx,
            Class<O> type, SparkOperatorTranslator<O> translator, UnaryPredicate<O> accept)
    {
      idx.put(type, new Translation<>(translator, accept));
    }
  }

  /* mapping of Euphoria operators to corresponding Flink transformations */
  private final Map<Class, Translation> translations = new IdentityHashMap<>();
  private final JavaSparkContext sparkEnv;

  public SparkFlowTranslator(JavaSparkContext sparkEnv) {
    this.sparkEnv = Objects.requireNonNull(sparkEnv);

    // basic operators
    Translation.set(translations, FlowUnfolder.InputOperator.class, new InputTranslator());
    Translation.set(translations, FlatMap.class, new FlatMapTranslator());
    Translation.set(translations, Repartition.class, new RepartitionTranslator());
    Translation.set(translations, ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    Translation.set(translations, Union.class, new UnionTranslator());

    // derived operators
    Translation.set(translations, ReduceByKey.class, new ReduceByKeyTranslator(),
            ReduceByKeyTranslator::wantTranslate);
  }

  @SuppressWarnings("unchecked")
  public List<DataSink<?>> translateInto(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<Operator<?, ?>> dag = flowToDag(flow);

    SparkExecutorContext executorContext =
            new SparkExecutorContext(sparkEnv, dag);

    // translate each operator to proper Spark transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Translation tx = translations.get(op.getClass());
      if (tx == null) {
        throw new UnsupportedOperationException(
                "Operator " + op.getClass().getSimpleName() + " not supported");
      }
      // ~ verify the flowToDag translation
      Preconditions.checkState(
              tx.accept == null || Boolean.TRUE.equals(tx.accept.apply(op)));

      JavaRDD<?> out = tx.translator.translate(op, executorContext);

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
              JavaRDD<WindowedElement> sparkOutput =
                      Objects.requireNonNull((JavaRDD) executorContext.getOutput(op));

              // unwrap data from WindowedElement
              JavaPairRDD<NullWritable, Object> unwrapped =
                      sparkOutput.mapToPair(el -> new Tuple2<>(NullWritable.get(), el.getElement()));


              try {
                Configuration conf = DataSinkOutputFormat.configure(
                        new Configuration(),
                        (DataSink) sink);

                conf.set(JobContext.OUTPUT_FORMAT_CLASS_ATTR,
                        DataSinkOutputFormat.class.getName());

                // FIXME blocking op
                unwrapped.saveAsNewAPIHadoopDataset(conf);
              } catch (IOException e) {
                throw new RuntimeException();
              }
            });

    return sinks;
  }

  /**
   * A functor to accept operators for translation if the  operator's
   * type equals a specified, fixed type. An optional custom "accept"
   * function can be provided to further tweak the decision whether
   * a particular operator instance is to be accepted for translation
   * or not.
   *
   * @param <O> the fixed operator type accepted
   */
  public static final class TranslateAcceptor<O>
          implements UnaryPredicate<Operator<?, ?>> {

    final Class<O> type;
    final UnaryPredicate<O> accept;

    public TranslateAcceptor(Class<O> type) {
      this (type, null);
    }

    public TranslateAcceptor(Class<O> type, UnaryPredicate<O> accept) {
      this.type = Objects.requireNonNull(type);
      this.accept = accept;
    }

    @Override
    public Boolean apply(Operator<?, ?> operator) {
      return type == operator.getClass()
              && (accept == null || accept.apply(type.cast(operator)));
    }
  }

  /**
   * Converts {@link Flow} to {@link DAG} of Flink specific {@link Operator}.
   *
   * <p>Invokes {@link #getAcceptors()} to determine which user provided
   * operators to accept for direct translation, i.e. which to leave in
   * the resulting DAG without expanding them to their {@link Operator#getBasicOps()}.
   */
  protected DAG<Operator<?, ?>> flowToDag(Flow flow) {
    // ~ get acceptors for translation
    Map<Class, Collection<TranslateAcceptor>> acceptors =
            buildAcceptorsIndex(getAcceptors());
    // ~ now, unfold the flow based on the specified acceptors
    return  FlowUnfolder.unfold(flow, operator -> {
      // accept the operator if any of the specified acceptors says so
      Collection<TranslateAcceptor> accs = acceptors.get(operator.getClass());
      if (accs != null && !accs.isEmpty()) {
        for (TranslateAcceptor acc : accs) {
          if (acc.apply(operator)) {
            return true;
          }
        }
      }
      return false;
    });
  }

  /**
   * Helper method to build an index over the given acceptors by
   * {@link TranslateAcceptor#type}.
   */
  private Map<Class, Collection<TranslateAcceptor>>
  buildAcceptorsIndex(Collection<TranslateAcceptor> accs) {
    IdentityHashMap<Class, Collection<TranslateAcceptor>> idx =
            new IdentityHashMap<>(accs.size());
    for (TranslateAcceptor<?> acc : accs) {
      Collection<TranslateAcceptor> cac =
              idx.computeIfAbsent(acc.type, k -> new ArrayList<>());
      cac.add(acc);
    }
    return idx;
  }

  protected Collection<TranslateAcceptor> getAcceptors() {
    return translations.entrySet().stream()
            .map(e -> new TranslateAcceptor(e.getKey(), e.getValue().accept))
            .collect(Collectors.toList());
  }
}
