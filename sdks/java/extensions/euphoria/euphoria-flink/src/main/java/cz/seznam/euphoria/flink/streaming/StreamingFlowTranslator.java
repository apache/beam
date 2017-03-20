/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming;

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
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlowTranslator;
import cz.seznam.euphoria.flink.streaming.io.DataSinkWrapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class StreamingFlowTranslator extends FlowTranslator {

  // static mapping of Euphoria operators to corresponding Flink transformations
  private static final Map<Class, StreamingOperatorTranslator> TRANSLATORS =
          new IdentityHashMap<>();

  static {
    defineTranslators();
  }

  @SuppressWarnings("unchecked")
  private static void defineTranslators() {
    // TODO add full support of all operators
    TRANSLATORS.put(FlowUnfolder.InputOperator.class, new InputTranslator());
    TRANSLATORS.put(FlatMap.class, new FlatMapTranslator());
    TRANSLATORS.put(Repartition.class, new RepartitionTranslator());
    TRANSLATORS.put(ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    TRANSLATORS.put(Union.class, new UnionTranslator());
  }

  // ~ ------------------------------------------------------------------------------

  private final StreamExecutionEnvironment env;
  private final Duration allowedLateness;
  private final Duration autoWatermarkInterval;

  public StreamingFlowTranslator(StreamExecutionEnvironment env,
                                 Duration allowedLateness,
                                 Duration autoWatermarkInterval) {
    this.env = Objects.requireNonNull(env);
    this.allowedLateness = Objects.requireNonNull(allowedLateness);
    this.autoWatermarkInterval = Objects.requireNonNull(autoWatermarkInterval);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Collection<TranslateAcceptor> getAcceptors() {
    return TRANSLATORS.keySet()
        .stream()
        .map(cls -> new TranslateAcceptor(cls)).collect(Collectors.toList());
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<DataSink<?>> translateInto(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<FlinkOperator<Operator<?, ?>>> dag = flowToDag(flow);

    // we're running exclusively on event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(autoWatermarkInterval.toMillis());

    StreamingExecutorContext executorContext =
        new StreamingExecutorContext(env,
                (DAG) dag,
                allowedLateness,
                env instanceof LocalStreamEnvironment);

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Operator<?, ?> originalOp = op.getOriginalOperator();
      StreamingOperatorTranslator<Operator> translator = TRANSLATORS.get((Class) originalOp.getClass());
      if (translator == null) {
        throw new UnsupportedOperationException(
                "Operator " + op.getClass().getSimpleName() + " not supported");
      }

      DataStream<?> out = translator.translate((FlinkOperator) op, executorContext);

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
}
