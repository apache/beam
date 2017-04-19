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
package org.apache.beam.runners.flink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Flink streaming overrides for various view (side input) transforms.
 */
class FlinkStreamingViewOverrides {

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsMap View.AsMap}
   * for the Flink runner in streaming mode.
   */
  static class StreamingViewAsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {

    private final transient FlinkRunner runner;

    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsMap(FlinkRunner runner, View.AsMap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, V>> expand(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, V>> view =
          PCollectionViews.mapView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(CreateFlinkPCollectionView.<KV<K, V>, Map<K, V>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMap";
    }
  }

  /**
   * Specialized expansion for {@link
   * View.AsMultimap View.AsMultimap} for the
   * Flink runner in streaming mode.
   */
  static class StreamingViewAsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {

    private final transient FlinkRunner runner;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsMultimap(FlinkRunner runner, View.AsMultimap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, Iterable<V>>> view =
          PCollectionViews.multimapView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(CreateFlinkPCollectionView.<KV<K, V>, Map<K, Iterable<V>>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMultimap";
    }
  }

  /**
   * Specialized implementation for
   * {@link View.AsList View.AsList} for the
   * Flink runner in streaming mode.
   */
  static class StreamingViewAsList<T>
      extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsList(FlinkRunner runner, View.AsList<T> transform) {}

    @Override
    public PCollectionView<List<T>> expand(PCollection<T> input) {
      PCollectionView<List<T>> view =
          PCollectionViews.listView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(CreateFlinkPCollectionView.<T, List<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsList";
    }
  }

  /**
   * Specialized implementation for
   * {@link View.AsIterable View.AsIterable} for the
   * Flink runner in streaming mode.
   */
  static class StreamingViewAsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsIterable(FlinkRunner runner, View.AsIterable<T> transform) { }

    @Override
    public PCollectionView<Iterable<T>> expand(PCollection<T> input) {
      PCollectionView<Iterable<T>> view =
          PCollectionViews.iterableView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(CreateFlinkPCollectionView.<T, Iterable<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsIterable";
    }
  }

  /**
   * Specialized expansion for
   * {@link View.AsSingleton View.AsSingleton} for the
   * Flink runner in streaming mode.
   */
  static class StreamingViewAsSingleton<T>
      extends PTransform<PCollection<T>, PCollectionView<T>> {
    private View.AsSingleton<T> transform;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsSingleton(FlinkRunner runner, View.AsSingleton<T> transform) {
      this.transform = transform;
    }

    @Override
    public PCollectionView<T> expand(PCollection<T> input) {
      Combine.Globally<T, T> combine = Combine.globally(
          new SingletonCombine<>(transform.hasDefaultValue(), transform.defaultValue()));
      if (!transform.hasDefaultValue()) {
        combine = combine.withoutDefaults();
      }
      return input.apply(combine.asSingletonView());
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsSingleton";
    }

    private static class SingletonCombine<T> extends Combine.BinaryCombineFn<T> {
      private boolean hasDefaultValue;
      private T defaultValue;

      SingletonCombine(boolean hasDefaultValue, T defaultValue) {
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
      }

      @Override
      public T apply(T left, T right) {
        throw new IllegalArgumentException("PCollection with more than one element "
            + "accessed as a singleton view. Consider using Combine.globally().asSingleton() to "
            + "combine the PCollection into a single value");
      }

      @Override
      public T identity() {
        if (hasDefaultValue) {
          return defaultValue;
        } else {
          throw new IllegalArgumentException(
              "Empty PCollection accessed as a singleton view. "
                  + "Consider setting withDefault to provide a default value");
        }
      }
    }
  }

  static class StreamingCombineGloballyAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {
    Combine.GloballyAsSingletonView<InputT, OutputT> transform;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingCombineGloballyAsSingletonView(
        FlinkRunner runner,
        Combine.GloballyAsSingletonView<InputT, OutputT> transform) {
      this.transform = transform;
    }

    @Override
    public PCollectionView<OutputT> expand(PCollection<InputT> input) {
      PCollection<OutputT> combined =
          input.apply(Combine.globally(transform.getCombineFn())
              .withoutDefaults()
              .withFanout(transform.getFanout()));

      PCollectionView<OutputT> view = PCollectionViews.singletonView(
          combined,
          combined.getWindowingStrategy(),
          transform.getInsertDefault(),
          transform.getInsertDefault()
              ? transform.getCombineFn().defaultValue() : null,
          combined.getCoder());
      return combined
          .apply(ParDo.of(new WrapAsList<OutputT>()))
          .apply(CreateFlinkPCollectionView.<OutputT, OutputT>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingCombineGloballyAsSingletonView";
    }
  }

  private static class WrapAsList<T> extends DoFn<T, List<T>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(Collections.singletonList(c.element()));
    }
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use by {@link StreamingViewAsMap}, {@link StreamingViewAsMultimap},
   * {@link StreamingViewAsList}, {@link StreamingViewAsIterable}.
   * They require the input {@link PCollection} fits in memory.
   * For a large {@link PCollection} this is expected to crash!
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<T>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }

  /**
   * Creates a primitive {@link PCollectionView}.
   *
   * <p>For internal use only by runner implementors.
   *
   * @param <ElemT> The type of the elements of the input PCollection
   * @param <ViewT> The type associated with the {@link PCollectionView} used as a side input
   */
  public static class CreateFlinkPCollectionView<ElemT, ViewT>
      extends PTransform<PCollection<List<ElemT>>, PCollectionView<ViewT>> {
    private PCollectionView<ViewT> view;

    private CreateFlinkPCollectionView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    public static <ElemT, ViewT> CreateFlinkPCollectionView<ElemT, ViewT> of(
        PCollectionView<ViewT> view) {
      return new CreateFlinkPCollectionView<>(view);
    }

    @Override
    public PCollectionView<ViewT> expand(PCollection<List<ElemT>> input) {
      return view;
    }
  }
}
