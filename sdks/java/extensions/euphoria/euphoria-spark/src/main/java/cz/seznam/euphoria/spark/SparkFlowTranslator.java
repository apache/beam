package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Translates given {@link Flow} into Spark execution environment
 */
public class SparkFlowTranslator {

  // static mapping of Euphoria operators to corresponding Flink transformations
  private static final Map<Class<? extends Operator<?, ?>>, SparkOperatorTranslator> TRANSLATORS =
          new IdentityHashMap<>();

  static {
    // TODO add full support of all operators
    TRANSLATORS.put((Class) FlowUnfolder.InputOperator.class, new InputTranslator());
    TRANSLATORS.put((Class) FlatMap.class, new FlatMapTranslator());
    TRANSLATORS.put((Class) Repartition.class, new RepartitionTranslator());
    TRANSLATORS.put((Class) ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    TRANSLATORS.put((Class) Union.class, new UnionTranslator());

    //TRANSLATORS.put((Class) ReduceByKey.class, new ReduceByKeyTranslator());
  }

  private final JavaSparkContext sparkEnv;

  public SparkFlowTranslator(JavaSparkContext sparkEnv) {
    this.sparkEnv = Objects.requireNonNull(sparkEnv);
  }

  public List<DataSink<?>> translateInto(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<Operator<?, ?>> dag = FlowUnfolder.unfold(flow, getSupportedOperators());

    SparkExecutorContext executorContext =
            new SparkExecutorContext(sparkEnv, dag);

    // translate each operator to proper Spark transformation
    dag.traverse().map(Node::get).forEach(originalOp -> {
      SparkOperatorTranslator translator = TRANSLATORS.get((Class) originalOp.getClass());
      if (translator == null) {
        throw new UnsupportedOperationException(
                "Operator " + originalOp.getClass().getSimpleName() + " not supported");
      }

      JavaRDD<?> out = translator.translate(originalOp, executorContext);

      // save output of current operator to context
      executorContext.setOutput(originalOp, out);
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
                      sparkOutput.mapToPair(el -> new Tuple2<>(NullWritable.get(), el.get()));


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

  public Set<Class<? extends Operator<?, ?>>> getSupportedOperators() {
    return TRANSLATORS.keySet();
  }
}
