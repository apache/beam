/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlowTranslator;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
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

  // mapping of Euphoria operators to corresponding Flink transformations
  private final Map<Class, StreamingOperatorTranslator> translators =
          new IdentityHashMap<>();

  // ~ ------------------------------------------------------------------------------

  private final Settings settings;
  private final StreamExecutionEnvironment env;
  private final FlinkAccumulatorFactory accumulatorFactory;
  private final Duration allowedLateness;
  private final Duration autoWatermarkInterval;

  public StreamingFlowTranslator(Settings settings,
                                 StreamExecutionEnvironment env,
                                 FlinkAccumulatorFactory accumulatorFactory,
                                 Duration allowedLateness,
                                 Duration autoWatermarkInterval) {
    this.settings = settings;
    this.env = Objects.requireNonNull(env);
    this.accumulatorFactory = Objects.requireNonNull(accumulatorFactory);
    this.allowedLateness = Objects.requireNonNull(allowedLateness);
    this.autoWatermarkInterval = Objects.requireNonNull(autoWatermarkInterval);

    translators.put(FlowUnfolder.InputOperator.class, new InputTranslator());
    translators.put(FlatMap.class, new FlatMapTranslator());
    translators.put(ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    translators.put(Union.class, new UnionTranslator());
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Collection<TranslateAcceptor> getAcceptors() {
    return translators.keySet()
        .stream()
        .map(cls -> new TranslateAcceptor(cls)).collect(Collectors.toList());
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<DataSink<?>> translateInto(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<FlinkOperator<Operator<?, ?>>> dag = flowToDag(flow);

    // we're running exclusively on ingestion time
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    env.getConfig().setAutoWatermarkInterval(autoWatermarkInterval.toMillis());

    StreamingExecutorContext executorContext =
        new StreamingExecutorContext(env,
                (DAG) dag,
                accumulatorFactory,
                settings,
                allowedLateness,
                env instanceof LocalStreamEnvironment);

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Operator<?, ?> originalOp = op.getOriginalOperator();
      StreamingOperatorTranslator<Operator> translator = translators.get((Class) originalOp.getClass());
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
