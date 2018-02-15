/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import cz.seznam.euphoria.beam.coder.PairCoder;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.type.TypeAwareReduceFunctor;
import cz.seznam.euphoria.core.client.type.TypeAwareUnaryFunction;
import cz.seznam.euphoria.core.client.type.TypeAwareUnaryFunctor;
import cz.seznam.euphoria.core.client.type.TypeHint;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterables;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import org.joda.time.Duration;

/**
 * Keeps track of mapping between Euphoria {@link Dataset} and {@link PCollection}.
 */
class BeamExecutorContext {

  private DAG<Operator<?, ?>> dag;
  private final Map<Dataset<?>, PCollection<?>> outputs = new HashMap<>();
  private final Pipeline pipeline;
  private final Duration allowedLateness;

  private final Settings settings;

  private final AccumulatorProvider.Factory accumulatorFactory;

  BeamExecutorContext(
      DAG<Operator<?, ?>> dag,
      AccumulatorProvider.Factory accumulatorFactory,
      Pipeline pipeline,
      Settings settings,
      Duration allowedLateness) {

    this.dag = dag;
    this.accumulatorFactory = accumulatorFactory;
    this.pipeline = pipeline;
    this.settings = settings;
    this.allowedLateness = allowedLateness;
  }

  <IN> PCollection<IN> getInput(Operator<IN, ?> operator) {
    System.err.println(" *** getting input of " + operator);
    return Iterables.getOnlyElement(getInputs(operator));
  }

  @SuppressWarnings("unchecked")
  <IN> List<PCollection<IN>> getInputs(Operator<IN, ?> operator) {
    System.err.println(
        " *** operator " + operator + " has parents "
            + dag.getNode(operator).getParents());
    return dag
        .getNode(operator)
        .getParents()
        .stream()
        .map(Node::get)
        .map(parent -> {
          final PCollection<IN> out = (PCollection<IN>) outputs.get(parent.output());
          if (out == null) {
            throw new IllegalArgumentException("Output missing for operator " + parent.getName());
          }
          return out;
        })
        .collect(toList());
  }

  @SuppressWarnings("unchecked")
  <T> Optional<PCollection<T>> getPCollection(Dataset<T> dataset) {
    return Optional.ofNullable((PCollection) outputs.get(dataset));
  }

  <T> void setPCollection(Dataset<T> dataset, PCollection<T> coll) {
    final PCollection<?> prev = outputs.put(dataset, coll);
    if (prev != null && prev != coll) {
      throw new IllegalStateException(
         "Dataset(" + dataset + ") already materialized.");
    }
    coll.setCoder(getOutputCoder(dataset));
  }

  Pipeline getPipeline() {
    return pipeline;
  }

  boolean strongTypingEnabled() {
    return false;
  }

  <IN, OUT> Coder<OUT> getCoder(UnaryFunction<IN, OUT> unaryFunction) {
    if (unaryFunction instanceof TypeAwareUnaryFunction) {
      return getCoder(((TypeAwareUnaryFunction<IN, OUT>) unaryFunction).getTypeHint());
    }
    if (strongTypingEnabled()) {
      throw new IllegalArgumentException(
          "Missing type information for function " + unaryFunction);
    }
    return new KryoCoder<>();
  }

  <IN, OUT> Coder<OUT> getCoder(UnaryFunctor<IN, OUT> unaryFunctor) {
    if (unaryFunctor instanceof TypeAwareUnaryFunctor) {
      return getCoder(((TypeAwareUnaryFunctor<IN, OUT>) unaryFunctor).getTypeHint());
    }
    if (strongTypingEnabled()) {
      throw new IllegalArgumentException(
          "Missing type information for funtion " + unaryFunctor);
    }
    return new KryoCoder<>();
  }

  <IN, OUT> Coder<OUT> getCoder(ReduceFunctor<IN, OUT> reduceFunctor) {
    if (reduceFunctor instanceof TypeAwareReduceFunctor) {
      return getCoder(((TypeAwareReduceFunctor<IN, OUT>) reduceFunctor).getTypeHint());
    }
    if (strongTypingEnabled()) {
      throw new IllegalArgumentException(
          "Missing type information for function " + reduceFunctor);
    }
    return new KryoCoder<>();
  }

  @SuppressWarnings("unchecked")
  private <T> Coder<T> getCoder(TypeHint<T> typeHint) {
    try {
      return pipeline.getCoderRegistry().getCoder(
          (TypeDescriptor<T>) TypeDescriptor.of(typeHint.getType()));
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException("Unable to provide coder for type hint.", e);
    }
  }

  AccumulatorProvider.Factory getAccumulatorFactory() {
    return accumulatorFactory;
  }

  Settings getSettings() {
    return settings;
  }

  @SuppressWarnings("unchecked")
  private <T> Coder<T> getOutputCoder(Dataset<T> dataset) {
    Operator<?, ?> op = dataset.getProducer();
    if (op instanceof FlatMap) {
      FlatMap<?, T> m = (FlatMap) op;
      return getCoder(m.getFunctor());
    } else if (op instanceof Union) {
      Union<T> u = (Union) op;
      Dataset<T> first = Objects.requireNonNull(
          Iterables.getFirst(u.listInputs(), null));
      return getOutputCoder(first);
    } else if (op instanceof ReduceByKey) {
      ReduceByKey rb = (ReduceByKey) op;
      return PairCoder.of(getCoder(rb.getKeyExtractor()), getCoder(rb.getReducer()));
    } else if (op instanceof ReduceStateByKey) {
      ReduceStateByKey rbsk = (ReduceStateByKey) op;
      // FIXME
      return new KryoCoder<>();
    } else if (op == null) {
      // FIXME
      return new KryoCoder<>();
    }
    // FIXME
    return new KryoCoder<>();
  }

  Duration getAllowedLateness(Operator<?, ?> operator) {
    return allowedLateness;
  }

  void setTranslationDAG(DAG<Operator<?, ?>> dag) {
    this.dag = dag;
  }

}
