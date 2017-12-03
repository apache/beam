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

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.beam.io.BeamWriteSink;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * This class converts Euphoria's {@code Flow} into Beam's Pipeline.
 */
class FlowTranslator {

  private static final Map<Class, OperatorTranslator> translators = new IdentityHashMap<>();

  static {
    translators.put(FlowUnfolder.InputOperator.class, new InputTranslator());
    translators.put(FlatMap.class, new FlatMapTranslator());
    translators.put(Union.class, new UnionTranslator());

    // extended operators
    translators.put(ReduceByKey.class, new ReduceByKeyTranslator());
    translators.put(ReduceStateByKey.class, new ReduceStateByKeyTranslator());
  }

  @SuppressWarnings("unchecked")
  static Pipeline toPipeline(Flow flow,
                             AccumulatorProvider.Factory accumulatorFactory,
                             PipelineOptions options) {

    final DAG<Operator<?, ?>> dag = FlowUnfolder.unfold(flow, operator ->
        translators.containsKey(operator.getClass())
    );

    final Pipeline pipeline = Pipeline.create(options);

    final BeamExecutorContext executorContext = new BeamExecutorContext(
        dag, accumulatorFactory, pipeline);

    // translate each operator to a beam transformation
    dag.traverse()
        .map(Node::get)
        .forEach(op -> {
          final OperatorTranslator translator = translators.get(op.getClass());
          if (translator == null) {
            throw new UnsupportedOperationException(
                "Operator " + op.getClass().getSimpleName() + " not supported");
          }
          executorContext.setOutput(op, translator.translate(op, executorContext));
        });

    // process sinks
    dag.getLeafs()
        .stream()
        .map(Node::get)
        .forEach(op -> {
          final PCollection pcs = executorContext.getOutput(op)
              .orElseThrow(ExceptionUtils.illegal("Dataset " + op.output() + " has not been " +
                  "materialized"));
          DataSink<?> sink = op.output().getOutputSink();
          pcs.apply(BeamWriteSink.wrap(sink));
        });

    return executorContext.getPipeline();
  }
}
