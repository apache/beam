/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.InvalidWindows;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.StreamingPCollectionViewWriterFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValueBase;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Transforms for creating {@link PCollectionView}s from {@link PCollection}s,
 * for consuming the contents of those {@link PCollection}s as side inputs
 * to {@link ParDo} transforms. These transforms support viewing a {@link PCollection}
 * as a single value, an iterable, a map, or a multimap.
 *
 * <p> For a {@link PCollection} that contains a single value of type {@code T}
 * per window, such as the output of {@link Combine#globally},
 * use {@link View#asSingleton()} to prepare it for use as a side input:
 *
 * <pre>
 * {@code
 * PCollectionView<A> output = someOtherPCollection
 *     .apply(Combine.globally(...))
 *     .apply(View.asSingleton());
 * }
 * </pre>
 *
 * <p> To iterate over an entire window of a {@link PCollection} via
 * side input, use {@link View#asIterable()}:
 *
 * <pre>
 * {@code
 * PCollectionView<Iterable<A>> output =
 *     somePCollection.apply(View.asIterable());
 * }
 * </pre>
 *
 * <p> To access a {@link PCollection PCollection<K, V>} as a
 * {@code Map<K, Iterable<V>>} side input, use {@link View#asMap()}:
 *
 * <pre>
 * {@code
 * PCollectionView<Map<K, Iterable<V>> output =
 *     somePCollection.apply(View.asMap());
 * }
 * </pre>
 *
 * <p> If a {@link PCollection PCollection<K, V>} is known to
 * have a single value for each key, then use
 * {@code View.AsMultimap#withSingletonValues View.asMap().withSingletonValues()}
 * to view it as a {@code Map<K, V>}:
 *
 * <pre>
 * {@code
 * PCollectionView<Map<K, V> output =
 *     somePCollection.apply(View.asMap().withSingletonValues());
 * }
 * </pre>
 *
 * <p> See {@link ParDo#withSideInputs} for details on how to access
 * this variable inside a {@link ParDo} over another {@link PCollection}.
 */
public class View {

  // Do not instantiate
  private View() { }

  /**
   * Returns a {@link AsSingleton} transform that takes a singleton
   * {@link PCollection} as input and produces a {@link PCollectionView}
   * of the single value, to be consumed as a side input.
   *
   * <p> If the input {@link PCollection} is empty,
   * throws {@link NoSuchElementException} in the consuming
   * {@link DoFn}.
   *
   * <p> If the input {@link PCollection} contains more than one
   * element, throws {@link IllegalArgumentException} in the
   * consuming {@link DoFn}.
   */
  public static <T> AsSingleton<T> asSingleton() {
    return new AsSingleton<>();
  }

  /**
   * Returns a {@link AsIterable} that takes a
   * {@link PCollection} as input and produces a {@link PCollectionView}
   * of the values, to be consumed as an iterable side input.
   */
  public static <T> AsIterable<T> asIterable() {
    return new AsIterable<>();
  }

  /**
   * Returns an {@link AsMultimap} that takes a {@link PCollection} as input
   * and produces a {@link PCollectionView} of the values to be consumed
   * as a {@code Map<K, Iterable<V>>} side input.
   *
   * <p> Currently, the resulting map is required to fit into memory.
   */
  public static <K, V> AsMultimap<K, V> asMap() {
    return new AsMultimap<K, V>();
  }

  /**
   * A {@link PTransform} that produces a {@link PCollectionView} of a singleton
   * {@link PCollection} yielding the single element it contains.
   *
   * <p> Instantiate via {@link View#asIterable}.
   */
  public static class AsIterable<T> extends PTransform<
      PCollection<T>, PCollectionView<Iterable<T>>> {
    private static final long serialVersionUID = 0;

    private AsIterable() { }

    @Override
    public PCollectionView<Iterable<T>> apply(
        PCollection<T> input) {
      if (input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
        return input.apply(Combine.globally(new Concatenate<T>()).asSingletonView());
      } else {
        return input.apply(
            new CreatePCollectionView<T, Iterable<T>>(
                new IterablePCollectionView<T>(
                    input.getPipeline(), input.getWindowingStrategy(), input.getCoder())));
      }
    }
  }

  /**
   * A {@link PTransform} that produces a {@link PCollectionView} of a singleton
   * {@link PCollection} yielding the single element it contains.
   *
   * <p> Instantiate via {@link View#asIterable}.
   */
  public static class AsSingleton<T>
      extends PTransform<PCollection<T>, PCollectionView<T>> {
    private static final long serialVersionUID = 0;
    private final T defaultValue;
    private final boolean hasDefault;

    private AsSingleton() {
      this.defaultValue = null;
      this.hasDefault = false;
    }

    private AsSingleton(T defaultValue) {
      this.defaultValue = defaultValue;
      this.hasDefault = true;
    }

    /**
     * Default value to return for windows with no value in them.
     */
    public AsSingleton<T> withDefaultValue(T defaultValue) {
      return new AsSingleton(defaultValue);
    }

    @Override
    public PCollectionView<T> apply(PCollection<T> input) {
      SingletonPCollectionView<T> view = new SingletonPCollectionView<T>(
          input.getPipeline(), input.getWindowingStrategy(), hasDefault, defaultValue,
          input.getCoder());

      CreatePCollectionView<T, T> createView = new CreatePCollectionView<>(view);

      if (input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
        return input
            .apply(ParDo.named("WrapAsList").of(new DoFn<T, List<T>>() {
                      private static final long serialVersionUID = 0;
                      @Override
                      public void processElement(ProcessContext c) {
                        c.output(Arrays.asList(c.element()));
                      }
                    }))
            .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, input.getCoder())))
            .apply(createView);
      } else {
        return input.apply(createView);
      }
    }
  }

  /**
   * A {@link PTransform} that produces a {@link PCollectionView} of a keyed {@link PCollection}
   * yielding a map of keys to all associated values.
   *
   * <p> Instantiate via {@link View#asMap}.
   */
  public static class AsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {
    private static final long serialVersionUID = 0;

    private AsMultimap() { }

    /**
     * Returns a PTransform creating a view as a {@code Map<K, V>} rather than a
     * {@code Map<K, Iterable<V>>}. Requires that the PCollection have only
     * one value per key.
     */
    public AsSingletonMap<K, V, V> withSingletonValues() {
      return new AsSingletonMap<K, V, V>(null);
    }

    /**
     * Returns a PTransform creating a view as a {@code Map<K, VO>} rather than a
     * {@code Map<K, Iterable<V>>} by applying the given combiner to the set of
     * values associated with each key.
     */
    public <VO> AsSingletonMap<K, V, VO> withCombiner(CombineFn<V, ?, VO> combineFn) {
      return new AsSingletonMap<K, V, VO>(combineFn);
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
      MultimapPCollectionView<K, V> view = new MultimapPCollectionView<K, V>(
          input.getPipeline(), input.getWindowingStrategy(), input.getCoder());

      CreatePCollectionView<KV<K, V>, Map<K, Iterable<V>>> createView =
          new CreatePCollectionView<>(view);

      if (input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
        return input
            .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
            .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, input.getCoder())))
            .apply(createView);
      } else {
        return input.apply(createView);
      }
    }
  }

  /**
   * A {@link PTransform} that produces a {@link PCollectionView} of a keyed {@link PCollection}
   * yielding a map of keys to a single associated values.
   *
   * <p> Instantiate via {@link View#asMap}.
   */
  public static class AsSingletonMap<K, VI, VO>
      extends PTransform<PCollection<KV<K, VI>>, PCollectionView<Map<K, VO>>> {
    private static final long serialVersionUID = 0;

    private CombineFn<VI, ?, VO> combineFn;

    private AsSingletonMap(CombineFn<VI, ?, VO> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public PCollectionView<Map<K, VO>> apply(PCollection<KV<K, VI>> input) {
      // VI == VO if combineFn is null
      @SuppressWarnings("unchecked")
      PCollection<KV<K, VO>> combined =
        combineFn == null
        ? (PCollection) input
        : input.apply(Combine.perKey(combineFn.<K>asKeyedFn()));

      MapPCollectionView<K, VO> view = new MapPCollectionView<K, VO>(
          input.getPipeline(), combined.getWindowingStrategy(), combined.getCoder());

      CreatePCollectionView<KV<K, VO>, Map<K, VO>> createView = new CreatePCollectionView<>(view);

      if (combined.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
        return combined
            .apply(Combine.globally(new Concatenate<KV<K, VO>>()).withoutDefaults())
            .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, combined.getCoder())))
            .apply(createView);
      } else {
        return combined.apply(createView);
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // Internal details below

  /**
   * Combiner that combines {@code T}s into a single {@code Iterable<T>} containing
   * all inputs.
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends CombineFn<T, List<T>, Iterable<T>> {
    private static final long serialVersionUID = 0;

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
    public Iterable<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }

  /**
   * Creates a primitive {@link PCollectionView}.
   *
   * <p> For internal use only.
   *
   * @param <R> The type of the elements of the input PCollection
   * @param <T> The type associated with the {@link PCollectionView} used as a side input
   */
  public static class CreatePCollectionView<R, T>
      extends PTransform<PCollection<R>, PCollectionView<T>> {
    private static final long serialVersionUID = 0;

    private PCollectionView<T> view;

    public CreatePCollectionView(PCollectionView<T> view) {
      this.view = view;
    }

    @Override
    public PCollectionView<T> apply(PCollection<R> input) {
      return view;
    }

    static {
      DirectPipelineRunner.registerDefaultTransformEvaluator(
          CreatePCollectionView.class,
          new DirectPipelineRunner.TransformEvaluator<CreatePCollectionView>() {
            @Override
            public void evaluate(
                CreatePCollectionView transform,
                DirectPipelineRunner.EvaluationContext context) {
              evaluateTyped(transform, context);
            }

            private <R, T> void evaluateTyped(
                CreatePCollectionView<R, T> transform,
                DirectPipelineRunner.EvaluationContext context) {
              List<WindowedValue<R>> elems =
                  context.getPCollectionWindowedValues(context.getInput(transform));
              context.setPCollectionView(context.getOutput(transform), elems);
            }
          });
    }
  }

  private static class SingletonPCollectionView<T>
      extends PCollectionViewBase<T> {
    private static final long serialVersionUID = 0;
    private byte[] encodedDefaultValue;
    private transient T defaultValue;
    private Coder<T> valueCoder;

    public SingletonPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy,
        boolean hasDefault, T defaultValue, Coder<T> valueCoder) {
      super(windowingStrategy, valueCoder);
      setPipelineInternal(pipeline);
      this.defaultValue = defaultValue;
      this.valueCoder = valueCoder;
      if (hasDefault) {
        try {
          this.encodedDefaultValue = CoderUtils.encodeToByteArray(valueCoder, defaultValue);
        } catch (IOException e) {
          throw new RuntimeException("Unexpected IOException: ", e);
        }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T fromIterableInternal(Iterable<WindowedValue<?>> contents) {
      if (encodedDefaultValue != null && defaultValue == null) {
        try {
          defaultValue = CoderUtils.decodeFromByteArray(valueCoder, encodedDefaultValue);
        } catch (IOException e) {
          throw new RuntimeException("Unexpected IOException: ", e);
        }
      }

      if (encodedDefaultValue != null && !contents.iterator().hasNext()) {
        return defaultValue;
      }
      try {
        return (T) Iterables.getOnlyElement(contents).getValue();
      } catch (NoSuchElementException exc) {
        throw new NoSuchElementException(
            "Empty PCollection accessed as a singleton view.");
      } catch (IllegalArgumentException exc) {
        throw new IllegalArgumentException(
            "PCollection with more than one element "
            + "accessed as a singleton view.");
      }
    }
  }

  private static class IterablePCollectionView<T>
      extends PCollectionViewBase<Iterable<T>> {
    private static final long serialVersionUID = 0;

    public IterablePCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy, Coder<T> valueCoder) {
      super(windowingStrategy, valueCoder);
      setPipelineInternal(pipeline);
    }

    @Override
    public Iterable<T> fromIterableInternal(Iterable<WindowedValue<?>> contents) {
      return Iterables.transform(contents, new Function<WindowedValue<?>, T>() {
        @SuppressWarnings("unchecked")
        @Override
        public T apply(WindowedValue<?> input) {
          return (T) input.getValue();
        }
      });
    }
  }

  private static class MultimapPCollectionView<K, V>
      extends PCollectionViewBase<Map<K, Iterable<V>>> {
    private static final long serialVersionUID = 0;

    public MultimapPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy, Coder<KV<K, V>> valueCoder) {
      super(windowingStrategy, valueCoder);
      setPipelineInternal(pipeline);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<K, Iterable<V>> fromIterableInternal(Iterable<WindowedValue<?>> contents) {
      Multimap<K, V> multimap = HashMultimap.create();
      for (WindowedValue<?> elem : contents) {
        KV<K, V> kv = (KV<K, V>) elem.getValue();
        multimap.put(kv.getKey(), kv.getValue());
      }
      // Don't want to promise in-memory or cheap Collection.size().
      return (Map) multimap.asMap();
    }
  }

  private static class MapPCollectionView<K, V>
      extends PCollectionViewBase<Map<K, V>> {
    private static final long serialVersionUID = 0;

    public MapPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy, Coder<KV<K, V>> valueCoder) {
      super(windowingStrategy, valueCoder);
      setPipelineInternal(pipeline);
    }

    @Override
    public Map<K, V> fromIterableInternal(Iterable<WindowedValue<?>> contents) {
      Map<K, V> map = new HashMap<>();
      for (WindowedValue<?> elem : contents) {
        @SuppressWarnings("unchecked")
        KV<K, V> kv = (KV<K, V>) elem.getValue();
        if (map.put(kv.getKey(), kv.getValue()) != null) {
          throw new IllegalArgumentException("Duplicate values for " + kv.getKey());
        }
      }
      return Collections.unmodifiableMap(map);
    }
  }

  private abstract static class PCollectionViewBase<T>
      extends PValueBase
      implements PCollectionView<T> {
    private static final long serialVersionUID = 0;

    PCollectionViewBase(WindowingStrategy<?, ?> windowingStrategy, Coder<?> valueCoder) {
      if (windowingStrategy.getWindowFn() instanceof InvalidWindows) {
        throw new IllegalArgumentException("WindowFn of PCollectionView cannot be InvalidWindows");
      }
      this.windowingStrategy = windowingStrategy;
      this.coder = (Coder)
          IterableCoder.of(WindowedValue.getFullCoder(
              valueCoder, windowingStrategy.getWindowFn().windowCoder()));
    }

    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      return tag;
    }

    @Override
    public WindowingStrategy<?, ?> getWindowingStrategyInternal() {
      return windowingStrategy;
    }

    @Override
    public Coder<Iterable<WindowedValue<?>>> getCoderInternal() {
      return coder;
    }

    private TupleTag<Iterable<WindowedValue<?>>> tag = new TupleTag<>();
    private WindowingStrategy<?, ?> windowingStrategy;
    private Coder<Iterable<WindowedValue<?>>> coder;
  }
}
