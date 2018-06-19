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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.Iterables;
import java.lang.invoke.MethodType;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.AbstractTypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareBinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareUnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareUnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.Node;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.KryoCoder;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.PairCoder;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keeps track of mapping between Euphoria {@link Dataset} and {@link PCollection}. */
class TranslationContext {

  private static final Logger LOG = LoggerFactory.getLogger(TranslationContext.class);
  private final Map<Dataset<?>, PCollection<?>> datasetToPCollection = new HashMap<>();
  private final Pipeline pipeline;
  private final Duration allowedLateness;
  private final Settings settings;
  private final AccumulatorProvider.Factory accumulatorFactory;
  private DAG<Operator<?, ?>> dag;

  TranslationContext(
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

  <InputT> PCollection<InputT> getInput(Operator<InputT, ?> operator) {
    return Iterables.getOnlyElement(getInputs(operator));
  }

  @SuppressWarnings("unchecked")
  <InputT> List<PCollection<InputT>> getInputs(Operator<InputT, ?> operator) {
    return dag.getNode(operator)
        .getParents()
        .stream()
        .map(Node::get)
        .map(
            parent -> {
              final PCollection<InputT> out =
                  (PCollection<InputT>) datasetToPCollection.get(parent.output());
              if (out == null) {
                throw new IllegalArgumentException(
                    "Output missing for operator " + parent.getName());
              }
              return out;
            })
        .collect(toList());
  }

  @SuppressWarnings("unchecked")
  <T> Optional<PCollection<T>> getPCollection(Dataset<T> dataset) {
    return Optional.ofNullable((PCollection) datasetToPCollection.get(dataset));
  }

  <T> void setPCollection(Dataset<T> dataset, PCollection<T> coll) {
    final PCollection<?> prev = datasetToPCollection.put(dataset, coll);
    if (prev != null && prev != coll) {
      throw new IllegalStateException("Dataset(" + dataset + ") already materialized.");
    }
    if (prev == null) {
      coll.setCoder(getOutputCoder(dataset));
    }
  }

  <T> void setFinishedPCollection(Dataset<T> dataset, PCollection<T> coll) {
    final PCollection<?> prev = datasetToPCollection.put(dataset, coll);
    if (prev != null && prev != coll) {
      throw new IllegalStateException("Dataset(" + dataset + ") already materialized.");
    }
  }

  Pipeline getPipeline() {
    return pipeline;
  }

  boolean strongTypingEnabled() {
    return false;
  }

  /**
   * Get return type of lambda function (e.g UnaryFunction).
   *
   * @return return type
   */
  static Type getLambdaReturnType(final Object lambda) {

    Type returnType = null;
    try {
      final Method method = lambda.getClass().getDeclaredMethod("writeReplace");
      method.setAccessible(true);
      final SerializedLambda serializedLambda = SerializedLambda.class.cast(method.invoke(lambda));
      final String signature = serializedLambda.getImplMethodSignature();
      final MethodType methodType =
          MethodType.fromMethodDescriptorString(signature, lambda.getClass().getClassLoader());
      returnType = methodType.returnType();
    } catch (IllegalAccessException
        | IllegalArgumentException
        | NoSuchMethodException
        | InvocationTargetException e) {
      // not a lambda try pure reflection, algo is trivial since it is functional interfaces: take
      // the only not Object methods ;)
      for (final Method m : lambda.getClass().getMethods()) {
        if (Object.class == m.getDeclaringClass()) {
          continue;
        }
        returnType = m.getReturnType();
        break;
      }
    }
    return returnType;
  }

  <InputT, OutputT> Coder<OutputT> getCoder(UnaryFunction<InputT, OutputT> unaryFunction) {

    if (unaryFunction instanceof TypeAwareUnaryFunction) {
      return getCoder(
          ((TypeAwareUnaryFunction<InputT, OutputT>) unaryFunction).getTypeDescriptor());
    }

    return inferCoderFromLambda(unaryFunction).orElseGet(()->getFallbackCoder(unaryFunction));
  }

  private <InputT, OutputT> Coder<OutputT> getCoderBasedOnFunctor(
      UnaryFunctor<InputT, OutputT> unaryFunctor) {
    if (unaryFunctor instanceof TypeAwareUnaryFunctor) {
      return getCoder(((TypeAwareUnaryFunctor<InputT, OutputT>) unaryFunctor).getTypeDescriptor());
    }

    return getFallbackCoder(unaryFunctor);
  }

  <FuncT, OutputT> Coder<OutputT> getFallbackCoder(FuncT function) {
    if (strongTypingEnabled()) {
      throw new IllegalArgumentException("Missing type information for function " + function);
    }
    return new KryoCoder<>();
  }

  /**
   * Tries to get OutputT type from unaryFunction and get coder from CoderRegistry. Will get coder
   * for standard Java types (Double, String, Long, Map, Integer, List etc.)
   *
   * @return coder for lambda return type
   */
  <InputT, OutputT> Optional<Coder<OutputT>> inferCoderFromLambda(
      UnaryFunction<InputT, OutputT> unaryFunction) {
    final Type lambdaType = getLambdaReturnType(unaryFunction);
    if (lambdaType != null) {
      try {

        return Optional.of(getCoder(lambdaType));
      } catch (IllegalArgumentException e) {
        // suppress exception, failed to infer coder from return type.
        LOG.debug("Couldn't infer coder for lambda return type {}", lambdaType);
      }
    }
    return Optional.empty();
  }

  <LeftT, RightT, OutputT> Coder<OutputT> getCoder(
      BinaryFunctor<LeftT, RightT, OutputT> binaryFunctor) {
    if (binaryFunctor instanceof TypeAwareBinaryFunctor) {
      return getCoder(
          ((TypeAwareBinaryFunctor<LeftT, RightT, OutputT>) binaryFunctor).getTypeDescriptor());
    }
    return getFallbackCoder(binaryFunctor);
  }

  <InputT, OutputT> Coder<OutputT> getCoder(ReduceFunctor<InputT, OutputT> reduceFunctor) {
    if (reduceFunctor instanceof AbstractTypeAware) {
      return getCoder(
          ((TypeAwareReduceFunctor<InputT, OutputT>) reduceFunctor).getTypeDescriptor());
    }
    return getFallbackCoder(reduceFunctor);
  }

  private <T> Coder<T> getCoder(TypeDescriptor<T> typeHint) {
    try {
      return pipeline.getCoderRegistry().getCoder(typeHint);
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException("Unable to provide coder for type hint.", e);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> Coder<T> getCoder(Type type) {
    return getCoder((TypeDescriptor<T>) TypeDescriptor.of(type));
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
      return getCoderBasedOnFunctor(m.getFunctor());
    } else if (op instanceof Union) {
      Union<T> u = (Union) op;
      Dataset<T> first = Objects.requireNonNull(Iterables.getFirst(u.listInputs(), null));
      return getOutputCoder(first);
    } else if (op instanceof ReduceByKey) {
      ReduceByKey rb = (ReduceByKey) op;
      Coder reducerCoder = getCoder(rb.getReducer());
      Coder keyCoder = getCoder(rb.getKeyExtractor());
      return PairCoder.of(keyCoder, reducerCoder);
    } else if (op instanceof ReduceStateByKey) {
      ReduceStateByKey rbsk = (ReduceStateByKey) op;
      // TODO
      return new KryoCoder<>();
    } else if (op instanceof WrappedInputPCollectionOperator) {
      return ((WrappedInputPCollectionOperator) op).input.getCoder();
    } else if (op == null) {
      // TODO
      return new KryoCoder<>();
    } else if (op instanceof Join) {
      Join join = (Join) op;
      Coder keyCoder = getCoder(join.getLeftKeyExtractor());
      Coder outputValueCoder = getCoder(join.getJoiner());
      return PairCoder.of(keyCoder, outputValueCoder);
    }
    // TODO
    return new KryoCoder<>();
  }

  Duration getAllowedLateness(Operator<?, ?> operator) {
    return allowedLateness;
  }

  void setTranslationDAG(DAG<Operator<?, ?>> dag) {
    this.dag = dag;
  }
}
