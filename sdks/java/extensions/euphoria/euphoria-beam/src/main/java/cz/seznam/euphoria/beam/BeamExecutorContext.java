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

import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.Operator;
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
import java.util.Optional;

import static java.util.stream.Collectors.toList;

/**
 * Keeps track of mapping between Euphoria {@link Operator} and {@link PCollection}.
 */
class BeamExecutorContext {

  private final DAG<Operator<?, ?>> dag;
  private final Map<Operator<?, ?>, PCollection<?>> outputs = new HashMap<>();
  private final Pipeline pipeline;

  private final Settings settings = new Settings();

  private final AccumulatorProvider.Factory accumulatorFactory;

  BeamExecutorContext(DAG<Operator<?, ?>> dag,
                      AccumulatorProvider.Factory accumulatorFactory,
                      Pipeline pipeline) {
    this.dag = dag;
    this.accumulatorFactory = accumulatorFactory;
    this.pipeline = pipeline;
  }

  <IN> PCollection<IN> getInput(Operator<IN, ?> operator) {
    return Iterables.getOnlyElement(getInputs(operator));
  }

  @SuppressWarnings("unchecked")
  <IN> List<PCollection<IN>> getInputs(Operator<IN, ?> operator) {
    return dag
        .getNode(operator)
        .getParents()
        .stream()
        .map(Node::get)
        .map(parent -> {
          final PCollection<IN> out = (PCollection<IN>) outputs.get(parent);
          if (out == null) {
            throw new IllegalArgumentException("Output missing for operator " + parent.getName());
          }
          return out;
        })
        .collect(toList());
  }

  Optional<PCollection<?>> getOutput(Operator<?, ?> operator) {
    return Optional.ofNullable(outputs.get(operator));
  }

  void setOutput(Operator<?, ?> operator, PCollection<?> output) {
    final PCollection<?> prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
         "Operator(" + operator.getName() + ") output already processed");
    }
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
      throw new IllegalArgumentException("x");

    }
    return new KryoCoder<>();
  }

  <IN, OUT> Coder<OUT> getCoder(UnaryFunctor <IN, OUT> unaryFunctor) {
    if (unaryFunctor instanceof TypeAwareUnaryFunctor) {
      return getCoder(((TypeAwareUnaryFunctor<IN, OUT>) unaryFunctor).getTypeHint());
    }
    if (strongTypingEnabled()) {
      throw new IllegalArgumentException("x");

    }
    return new KryoCoder<>();
  }

  <IN, OUT> Coder<OUT> getCoder(ReduceFunctor <IN, OUT> reduceFunctor) {
    if (reduceFunctor instanceof TypeAwareReduceFunctor) {
      return getCoder(((TypeAwareReduceFunctor<IN, OUT>) reduceFunctor).getTypeHint());
    }
    if (strongTypingEnabled()) {
      throw new IllegalArgumentException("x");

    }
    return new KryoCoder<>();
  }

  @SuppressWarnings("unchecked")
  <T> Coder<T> getCoder(TypeHint<T> typeHint) {
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
}
