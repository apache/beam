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
package org.apache.beam.runners.dataflow.internal;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Specialized implementation of {@code GroupByKey} for translating Redistribute transform into
 * Dataflow service protos.
 */
public class DataflowGroupByKey<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  // Plumbed from Redistribute transform.
  private final boolean allowDuplicates;

  private DataflowGroupByKey(boolean allowDuplicates) {
    this.allowDuplicates = allowDuplicates;
  }

  /**
   * Returns a {@code DataflowGroupByKey<K, V>} {@code PTransform}.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  public static <K, V> DataflowGroupByKey<K, V> create() {
    return new DataflowGroupByKey<>(false);
  }

  /**
   * Returns a {@code DataflowGroupByKey<K, V>} {@code PTransform} that its output can have
   * duplicated elements.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  public static <K, V> DataflowGroupByKey<K, V> createWithAllowDuplicates() {
    return new DataflowGroupByKey<>(true);
  }

  /** Returns whether it allows duplicated elements in the output. */
  public boolean allowDuplicates() {
    return allowDuplicates;
  }

  /////////////////////////////////////////////////////////////////////////////

  public static void applicableTo(PCollection<?> input) {
    WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
    // Verify that the input PCollection is bounded, or that there is windowing/triggering being
    // used. Without this, the watermark (at end of global window) will never be reached.
    if (windowingStrategy.getWindowFn() instanceof GlobalWindows
        && windowingStrategy.getTrigger() instanceof DefaultTrigger
        && input.isBounded() != IsBounded.BOUNDED) {
      throw new IllegalStateException(
          "DataflowGroupByKey cannot be applied to non-bounded PCollection in the GlobalWindow"
              + " without a trigger. Use a Window.into or Window.triggering transform prior to"
              + " DataflowGroupByKey.");
    }
  }

  public WindowingStrategy<?, ?> updateWindowingStrategy(WindowingStrategy<?, ?> inputStrategy) {
    // If the WindowFn was merging, set the bit to indicate it is already merged.
    // Switch to the continuation trigger associated with the current trigger.
    return inputStrategy
        .withAlreadyMerged(!inputStrategy.getWindowFn().isNonMerging())
        .withTrigger(inputStrategy.getTrigger().getContinuationTrigger());
  }

  @Override
  public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
    applicableTo(input);

    // Verify that the input Coder<KV<K, V>> is a KvCoder<K, V>, and that
    // the key coder is deterministic.
    Coder<K> keyCoder = getKeyCoder(input.getCoder());
    try {
      keyCoder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new IllegalStateException(
          "the keyCoder of a DataflowGroupByKey must be deterministic", e);
    }

    // This primitive operation groups by the combination of key and window,
    // merging windows as needed, using the windows assigned to the
    // key/value input elements and the window merge operation of the
    // window function associated with the input PCollection.
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(),
        updateWindowingStrategy(input.getWindowingStrategy()),
        input.isBounded(),
        getOutputKvCoder(input.getCoder()));
  }

  /**
   * Returns the {@code Coder} of the input to this transform, which should be a {@code KvCoder}.
   */
  @SuppressWarnings("unchecked")
  static <K, V> KvCoder<K, V> getInputKvCoder(Coder<?> inputCoder) {
    if (!(inputCoder instanceof KvCoder)) {
      throw new IllegalStateException("DataflowGroupByKey requires its input to use KvCoder");
    }
    return (KvCoder<K, V>) inputCoder;
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the {@code Coder} of the keys of the input to this transform, which is also used as the
   * {@code Coder} of the keys of the output of this transform.
   */
  static <K, V> Coder<K> getKeyCoder(Coder<KV<K, V>> inputCoder) {
    return DataflowGroupByKey.<K, V>getInputKvCoder(inputCoder).getKeyCoder();
  }

  /** Returns the {@code Coder} of the values of the input to this transform. */
  public static <K, V> Coder<V> getInputValueCoder(Coder<KV<K, V>> inputCoder) {
    return DataflowGroupByKey.<K, V>getInputKvCoder(inputCoder).getValueCoder();
  }

  /** Returns the {@code Coder} of the {@code Iterable} values of the output of this transform. */
  static <K, V> Coder<Iterable<V>> getOutputValueCoder(Coder<KV<K, V>> inputCoder) {
    return IterableCoder.of(getInputValueCoder(inputCoder));
  }

  /** Returns the {@code Coder} of the output of this transform. */
  public static <K, V> KvCoder<K, Iterable<V>> getOutputKvCoder(Coder<KV<K, V>> inputCoder) {
    return KvCoder.of(getKeyCoder(inputCoder), getOutputValueCoder(inputCoder));
  }

  static class DataflowGroupByKeyTranslator
      implements PTransformTranslation.TransformPayloadTranslator<DataflowGroupByKey<?, ?>> {
    @Override
    public String getUrn() {
      return PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN;
    }

    @Override
    @SuppressWarnings("nullness")
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, DataflowGroupByKey<?, ?>> transform, SdkComponents components) {
      return RunnerApi.FunctionSpec.newBuilder().setUrn(getUrn(transform.getTransform())).build();
    }
  }

  /** Registers {@link DataflowGroupByKeyTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    @SuppressWarnings("rawtypes")
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(DataflowGroupByKey.class, new DataflowGroupByKeyTranslator());
    }
  }
}
