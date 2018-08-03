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
import java.lang.reflect.TypeVariable;
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
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Named;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.Node;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.EuphoriaCoderProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.KryoCoder;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.PairCoder;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.RegisterCoders;
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
  private final boolean allowKryoCoderAsFallback;
  private EuphoriaCoderProvider euphoriaCoderProvider;

  TranslationContext(
      DAG<Operator<?, ?>> dag,
      AccumulatorProvider.Factory accumulatorFactory,
      Pipeline pipeline,
      Settings settings,
      Duration allowedLateness,
      boolean allowKryoCoderAsFallback) {

    this.dag = dag;
    this.accumulatorFactory = accumulatorFactory;
    this.pipeline = pipeline;
    this.settings = settings;
    this.allowedLateness = allowedLateness;
    this.allowKryoCoderAsFallback = allowKryoCoderAsFallback;
  }

  public void setEuphoriaCoderProvider(EuphoriaCoderProvider coderProvider) {
    Objects.requireNonNull(coderProvider);
    if (this.euphoriaCoderProvider != null) {
      throw new IllegalStateException(
          String.format(
              "%s already set. Use %s only once per %s.",
              EuphoriaCoderProvider.class.getSimpleName(),
              RegisterCoders.class.getSimpleName(),
              BeamFlow.class.getSimpleName()));
    }

    pipeline.getCoderRegistry().registerCoderProvider(coderProvider);

    this.euphoriaCoderProvider = coderProvider;
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

    if (prev != null) {
      return;
    }

    TypeDescriptor<T> collElementType = coll.getTypeDescriptor();
    if (collElementType != null) {
      coll.setCoder(getCoderForTypeOrFallbackCoder(collElementType));
    } else {
      coll.setCoder(getCoderBasedOnDatasetElementType(dataset));
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

  public <T> Coder<T> getOutputCoder(TypeAware.Output<T> outputTypeAware) {
    TypeDescriptor<T> outputType = outputTypeAware.getOutputType();

    return getCoderForTypeOrFallbackCoder(outputTypeAware, outputType, "Output");
  }

  public <T> Coder<T> getValueCoder(TypeAware.Value<T> valueTypeAware) {
    TypeDescriptor<T> valueType = valueTypeAware.getValueType();
    return getCoderForTypeOrFallbackCoder(valueTypeAware, valueType, "Value");
  }

  public <T> Coder<T> getKeyCoder(TypeAware.Key<T> keyTypeAware) {
    TypeDescriptor<T> keyType = keyTypeAware.getKeyType();
    return getCoderForTypeOrFallbackCoder(keyTypeAware, keyType, "Key");
  }

  private <T> Coder<T> getCoderForTypeOrFallbackCoder(
      Named namedTypeSource, TypeDescriptor<T> type, String typeName) {

    if (type == null) {
      if (allowKryoCoderAsFallback) {
        return createFallbackKryoCoder();
      } else {
        throw new IllegalStateException(
            String.format(
                "%s type of '%s'(%s) is unknown and use of fallback Kryo coder is not allowed.",
                typeName, namedTypeSource.getName(), namedTypeSource.getClass().getSimpleName()));
      }
    }

    Class<? super T> rawType = type.getRawType();
    if (Pair.class.isAssignableFrom(rawType)) {
      @SuppressWarnings("unchecked")
      TypeDescriptor<Pair> pairType = (TypeDescriptor<Pair>) type;
      @SuppressWarnings("unchecked")
      Coder<T> pairCoder = createPairCoderIfKeyAndValueCodersAvailable(pairType);
      return pairCoder;
    }

    return getCoderForTypeOrFallbackCoder(type);
  }

  private PairCoder createPairCoderIfKeyAndValueCodersAvailable(TypeDescriptor<Pair> type) {

    TypeVariable<Class<Pair>>[] pairTypeParams = Pair.class.getTypeParameters();

    TypeDescriptor<?> keyType = type.resolveType(pairTypeParams[0]);
    TypeDescriptor<?> valueType = type.resolveType(pairTypeParams[1]);

    Coder<?> keyCoder = getCoderForTypeOrFallbackCoder(keyType);
    Coder<?> valueCoder = getCoderForTypeOrFallbackCoder(valueType);

    return PairCoder.of(keyCoder, valueCoder);
  }

  public <T> Coder<T> getCoderForTypeOrFallbackCoder(TypeDescriptor<T> typeHint) {

    if (typeHint == null) {
      return createFallbackKryoCoder();
    }

    try {
      return pipeline.getCoderRegistry().getCoder(typeHint);
    } catch (CannotProvideCoderException e) {
      LOG.info(String.format("Unable to fetch coder for '%s' failing back to Kryo.", typeHint));
      return createFallbackKryoCoder();
    }
  }

  AccumulatorProvider.Factory getAccumulatorFactory() {
    return accumulatorFactory;
  }

  Settings getSettings() {
    return settings;
  }

  public <T> Coder<T> getCoderBasedOnDatasetElementType(Dataset<T> dataset) {
    TypeDescriptor<T> elementType = TypeUtils.getDatasetElementType(dataset);

    if (elementType == null) {
      Optional<PCollection<T>> optionalPCollection = getPCollection(dataset);
      if (optionalPCollection.isPresent()) {
        elementType = optionalPCollection.get().getTypeDescriptor();
      }
    }

    return getCoderForTypeOrFallbackCoder(elementType);
  }

  private <T> KryoCoder<T> createFallbackKryoCoder() {

    if (!allowKryoCoderAsFallback) {
      throw new IllegalStateException("Using Kryo as fallback coder is not allowed.");
    }

    return createKryoCoder();
  }

  public <T> KryoCoder<T> createKryoCoder() {

    if (euphoriaCoderProvider != null) {
      return euphoriaCoderProvider.createKryoCoderWithRegisteredClasses();
    }

    return KryoCoder.withoutClassRegistration();
  }

  Duration getAllowedLateness(Operator<?, ?> operator) {
    return allowedLateness;
  }

  void setTranslationDAG(DAG<Operator<?, ?>> dag) {
    this.dag = dag;
  }

  public boolean isUseOfKryoCoderAsFallbackAllowed() {
    return allowKryoCoderAsFallback;
  }
}
