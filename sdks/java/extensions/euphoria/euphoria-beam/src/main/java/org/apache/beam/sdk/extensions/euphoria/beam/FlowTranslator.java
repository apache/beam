/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.beam;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.beam.io.BeamWriteSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.DataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.extensions.euphoria.core.executor.FlowUnfolder;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.Node;
import org.apache.beam.sdk.extensions.euphoria.core.util.ExceptionUtils;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * This class converts Euphoria's {@code Flow} into Beam's Pipeline.
 */
class FlowTranslator {

  private static final Multimap<Class, OperatorTranslator> translators = ArrayListMultimap.create();

  //Note that when there are more than one translator ordering defines priority.
  //First added to `translators` is first asked whenever it can translate the operator.
  static {
    translators.put(FlowUnfolder.InputOperator.class, new InputTranslator());
    translators.put(FlatMap.class, new FlatMapTranslator());
    translators.put(Union.class, new UnionTranslator());
    translators.put(WrappedPCollectionOperator.class, WrappedPCollectionOperator::translate);

    // extended operators
    translators.put(ReduceByKey.class, new ReduceByKeyTranslator());
    translators.put(ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    translators.put(Join.class, new JoinTranslator());
  }

  @SuppressWarnings("unchecked")
  private static boolean isOperatorDirectlyTranslatable(Operator operator){
    Collection<OperatorTranslator> availableTranslators = translators.get(operator.getClass());
    if (availableTranslators.isEmpty()){
      return false;
    }

    for (OperatorTranslator translator : availableTranslators){
      if (translator.canTranslate(operator)){
        return true;
      }
    }

    return false;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static OperatorTranslator getTranslatorIfAvailable(Operator operator){
    Collection<OperatorTranslator> availableTranslators = translators.get(operator.getClass());
    if (availableTranslators.isEmpty()){
      return null;
    }

    for (OperatorTranslator translator : availableTranslators){
      if (translator.canTranslate(operator)){
        return translator;
      }
    }

    return null;
  }

  static Pipeline toPipeline(
      Flow flow,
      AccumulatorProvider.Factory accumulatorFactory,
      PipelineOptions options,
      Settings settings,
      Duration allowedLateness) {

    final Pipeline pipeline = Pipeline.create(options);
    DAG<Operator<?, ?>> dag = toDAG(flow);

    final BeamExecutorContext executorContext =
        new BeamExecutorContext(dag, accumulatorFactory, pipeline, settings, allowedLateness);

    updateContextBy(dag, executorContext);
    return executorContext.getPipeline();
  }

  static DAG<Operator<?, ?>> toDAG(Flow flow) {
    final DAG<Operator<?, ?>> dag =
        FlowUnfolder.unfold(flow, FlowTranslator::isOperatorDirectlyTranslatable);
    return dag;
  }

  static DAG<Operator<?, ?>> unfold(DAG<Operator<?, ?>> dag) {
    return FlowUnfolder.translate(dag, FlowTranslator::isOperatorDirectlyTranslatable);
  }

  @SuppressWarnings("unchecked")
  static void updateContextBy(DAG<Operator<?, ?>> dag, BeamExecutorContext context) {

    // translate each operator to a beam transformation
    dag.traverse()
        .map(Node::get)
        .forEach(
            op -> {
              final OperatorTranslator translator = getTranslatorIfAvailable(op);
              if (translator == null) {
                throw new UnsupportedOperationException(
                    "Operator " + op.getClass().getSimpleName() + " not supported");
              }
              context.setPCollection(op.output(), translator.translate(op, context));
            });

    // process sinks
    dag.getLeafs()
        .stream()
        .map(Node::get)
        .forEach(
            op -> {
              final PCollection pcs =
                  context
                      .getPCollection(op.output())
                      .orElseThrow(
                          ExceptionUtils.illegal(
                              "Dataset " + op.output() + " has not been " + "materialized"));
              DataSink<?> sink = op.output().getOutputSink();
              if (sink != null) {
                // the leaf might be consumed by some other Beam transformation
                // so the sink might be null
                pcs.apply(BeamWriteSink.wrap(sink));
              }
            });
  }
}
