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
 * PCollectionView<T> output = someOtherPCollection
 *     .apply(Combine.globally(...))
 *     .apply(View.asSingleton());
 * }
 * </pre>
 *
 * <p> For a small {@link PCollection} that can fit entirely in memory,
 * use {@link View#asList()} to prepare it for use as a {@code List}.
 * When read as a side input, the entire list will be cached in memory.
 *
 * <pre>
 * {@code
 * PCollectionView<List<T>> output =
 *    smallPCollection.apply(View.asList());
 * }
 * </pre>
 *
 * <p> If a {@link PCollection} of {@code KV<K, V>} is known to
 * have a single value for each key, then use {@link View#asMap()}
 * to view it as a {@code Map<K, V>}:
 *
 * <pre>
 * {@code
 * PCollectionView<Map<K, V> output =
 *     somePCollection.apply(View.asMap());
 * }
 * </pre>
 *
 * <p> Otherwise, to access a {@link PCollection} of {@code KV<K, V>} as a
 * {@code Map<K, Iterable<V>>} side input, use {@link View#asMultimap()}:
 *
 * <pre>
 * {@code
 * PCollectionView<Map<K, Iterable<V>> output =
 *     somePCollection.apply(View.asMap());
 * }
 * </pre>
 *
 * <p> To iterate over an entire window of a {@link PCollection} via
 * side input, use {@link View#asIterable()}:
 *
 * <pre>
 * {@code
 * PCollectionView<Iterable<T>> output =
 *     somePCollection.apply(View.asIterable());
 * }
 * </pre>
 *
 *
 * <p> Both {@link View#asMultimap()} and {@link View#asMap()} are useful
 * for implementing lookup based "joins" with the main input, when the
 * side input is small enough to fit into memory.
 *
 * <p> For example, if you represent a page on a website via some {@code Page} object and
 * have some type {@code UrlVisits} logging that a URL was visited, you could convert these
 * to more fully structured {@code PageVisit} objects using a side input, something like the
 * following:
 *
 * <pre>
 * {@code
 * PCollection<Page> pages = ... // pages fit into memory
 * PCollection<UrlVisit> urlVisits = ... // very large collection
 * final PCollectionView<Map<URL, Page>> = urlToPage
 *     .apply(WithKeys.of( ... )) // extract the URL from the page
 *     .apply(View.asMap());
 *
 * PCollection PageVisits = urlVisits
 *     .apply(ParDo.withSideInputs(urlToPage)
 *         .of(new DoFn<UrlVisit, PageVisit>() {
 *             {@literal @}Override
 *             void processElement(ProcessContext context) {
 *               UrlVisit urlVisit = context.element();
 *               Page page = urlToPage.get(urlVisit.getUrl());
 *               c.output(new PageVisit(page, urlVisit.getVisitData()));
 *             }
 *         }));
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
   * <pre>
   * {@code
   * PCollection<InputT> input = ...
   * CombineFn<InputT, OutputT> yourCombineFn = ...
   * PCollectionView<OutputT> output = input
   *     .apply(Combine.globally(yourCombineFn))
   *     .apply(View.asSingleton());
   * }</pre>
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
   * Returns a transform that takes a {@link PCollection} and returns a
   * {@code List} containing all of its elements, to be consumed as
   * a side input.
   *
   * <p> The resulting list is required to fit in memory.
   */
  public static <T> PTransform<PCollection<T>, PCollectionView<List<T>>> asList() {
    return Combine.globally(new Concatenate<T>()).asSingletonView();
  }

  /**
   * Returns a {@link AsIterable} that takes a
   * {@link PCollection} as input and produces a {@link PCollectionView}
   * of the values, to be consumed as an iterable side input.  The values of
   * this {@code Iterable} may not be cached; if that behavior is desired, use
   * {@link #asList}.
   */
  public static <T> AsIterable<T> asIterable() {
    return new AsIterable<>();
  }

  /**
   * Returns an {@link AsMap} transform that takes a {@link PCollection} as input
   * and produces a {@link PCollectionView} of the values to be consumed
   * as a {@code Map<K, V>} side input. It is required that each key of the input be
   * associated with a single value. If this is not the case, precede this
   * view with {@code Combine.perKey}, as below, or alternatively use {@link View#asMultimap()}.
   *
   * <pre>
   * {@code
   * PCollection<KV<K, V>> input = ...
   * CombineFn<V, OutputT> yourCombineFn = ...
   * PCollectionView<Map<K, OutputT>> output = input
   *     .apply(Combine.perKey(yourCombineFn.<K>asKeyedFn()))
   *     .apply(View.asMap());
   * }</pre>
   *
   * <p> Currently, the resulting map is required to fit into memory.
   */
  public static <K, V> AsMap<K, V> asMap() {
    return new AsMap<K, V>();
  }

  /**
   * Returns an {@link AsMultimap} transform that takes a {@link PCollection}
   * of {@code KV<K, V>} pairs as input and produces a {@link PCollectionView} of
   * its contents as a {@code Map<K, Iterable<V>>} for use as a side input.
   * In contrast to {@link View#asMap()}, it is not required that the keys in the
   * input collection be unique.
   *
   * <pre>
   * {@code
   * PCollection<KV<K, V>> input = ... // maybe more than one occurrence of a some keys
   * PCollectionView<Map<K, V>> output = input.apply(View.asMultimap());
   * }</pre>
   *
   * <p> Currently, the resulting map is required to fit into memory.
   */
  public static <K, V> AsMultimap<K, V> asMultimap() {
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

        // safe covariant cast List<T> -> Iterable<T>
        // not expressible in java, even with unchecked casts
        @SuppressWarnings({"rawtypes", "unchecked"})
        Combine.GloballyAsSingletonView<T, Iterable<T>> concatAndView =
            (Combine.GloballyAsSingletonView)
            Combine.globally(new Concatenate<T>()).asSingletonView();;

        return input.apply(concatAndView);
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
  public static class AsSingleton<T> extends PTransform<PCollection<T>, PCollectionView<T>> {
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
   * yielding a map from each key to its unique associated value. When converting
   * a {@link PCollection} that has more than one value per key, precede this transform with a
   * {@code Combine.perKey}:
   *
   * <pre>
   * {@code
   * PCollectionView<Map<K, OutputT>> input
   *     .apply(Combine.perKey(myCombineFunction))
   *     .apply(View.asMap());
   * }</pre>
   *
   * <p> Instantiate via {@link View#asMap}.
   */
  public static class AsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {
    private static final long serialVersionUID = 0;

    private AsMap() { }

    /**
     * @deprecated this method simply returns this AsMap unmodified
     */
    @Deprecated()
    public AsMap<K, V> withSingletonValues() {
      return this;
    }

    @Override
    public PCollectionView<Map<K, V>> apply(PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")

      MapPCollectionView<K, V> view = new MapPCollectionView<K, V>(
          input.getPipeline(), input.getWindowingStrategy(), input.getCoder());

      CreatePCollectionView<KV<K, V>, Map<K, V>> createView =
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

  ////////////////////////////////////////////////////////////////////////////
  // Internal details below

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing
   * all inputs.
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends CombineFn<T, List<T>, List<T>> {
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
   * <p> For internal use only by runner implementors.
   *
   * @param <ElemT> The type of the elements of the input PCollection
   * @param <ViewT> The type associated with the {@link PCollectionView} used as a side input
   */
  public static class CreatePCollectionView<ElemT, ViewT>
      extends PTransform<PCollection<ElemT>, PCollectionView<ViewT>> {
    private static final long serialVersionUID = 0;

    private PCollectionView<ViewT> view;

    public CreatePCollectionView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    @Override
    public PCollectionView<ViewT> apply(PCollection<ElemT> input) {
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

            private <ElemT, ViewT> void evaluateTyped(
                CreatePCollectionView<ElemT, ViewT> transform,
                DirectPipelineRunner.EvaluationContext context) {
              List<WindowedValue<ElemT>> elems =
                  context.getPCollectionWindowedValues(context.getInput(transform));
              context.setPCollectionView(context.getOutput(transform), elems);
            }
          });
    }
  }

  /**
   * Private implementation of conversion {@code Iterable<WindowedValue<T>>} to {@code T}.
   */
  private static class SingletonPCollectionView<T> extends PCollectionViewBase<T> {
    private static final long serialVersionUID = 0;
    private byte[] encodedDefaultValue;
    private transient T defaultValue;
    private Coder<T> valueCoder;

    public SingletonPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy,
        boolean hasDefault, T defaultValue, Coder<T> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
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

    /**
     * Input iterable must actually be {@code Iterable<WindowedValue<T>>}.
     */
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

  /**
   * Private implementation of conversion {@code Iterable<WindowedValue<T>>} to {@code Iterable<T>}.
   */
  private static class IterablePCollectionView<T> extends PCollectionViewBase<Iterable<T>> {
    private static final long serialVersionUID = 0;

    public IterablePCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy, Coder<T> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    /**
     * Input iterable must actually be {@code Iterable<WindowedValue<T>>}.
     */
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

  /**
   * Private implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>>}
   * to {@code Map<K, Iterable<V>>}.
   */
  private static class MultimapPCollectionView<K, V>
      extends PCollectionViewBase<Map<K, Iterable<V>>> {
    private static final long serialVersionUID = 0;

    public MultimapPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy, Coder<KV<K, V>> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    /**
     * Input iterable must actually be {@code Iterable<WindowedValue<KV<K, V>>>}.
     */
    @Override
    public Map<K, Iterable<V>> fromIterableInternal(Iterable<WindowedValue<?>> contents) {
      Multimap<K, V> multimap = HashMultimap.create();
      for (WindowedValue<?> elem : contents) {
        @SuppressWarnings("unchecked")
        KV<K, V> kv = (KV<K, V>) elem.getValue();
        multimap.put(kv.getKey(), kv.getValue());
      }

      // Safe covariant cast that Java cannot express without rawtypes, even with unchecked casts
      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<K, Iterable<V>> resultMap = (Map) multimap.asMap();
      return resultMap;
    }
  }

  /**
   * Private implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>} to
   * {@code Map<K, V>}.
   */
  private static class MapPCollectionView<K, V> extends PCollectionViewBase<Map<K, V>> {
    private static final long serialVersionUID = 0;

    public MapPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, ?> windowingStrategy, Coder<KV<K, V>> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    /**
     * Input iterable must actually be {@code Iterable<WindowedValue<KV<K, V>>>}.
     */
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

  /**
   * Base class for new implementations of side input views.
   *
   * <p>To implement a {@code PCollectionView<ViewT>} built from a {@code PCollection<ElemT>},
   * override {@code fromIterableInternal} with a function from
   * {@code Iterable<WindowedValue<ElemT>>} to {@code ViewT}.
   *
   * <p>This base class provides initialization and getters for a few boilerplate fields.
   */
  private abstract static class PCollectionViewBase<T>
      extends PValueBase
      implements PCollectionView<T> {
    private static final long serialVersionUID = 0;

    // for serialization only
    protected PCollectionViewBase() {
      super();
    }

    protected PCollectionViewBase(
        Pipeline pipeline,
        WindowingStrategy<?, ?> windowingStrategy,
        Coder<?> valueCoder) {

      super(pipeline);

      if (windowingStrategy.getWindowFn() instanceof InvalidWindows) {
        throw new IllegalArgumentException("WindowFn of PCollectionView cannot be InvalidWindows");
      }
      this.windowingStrategy = windowingStrategy;
      this.coder = (Coder)
          IterableCoder.of(WindowedValue.getFullCoder(
              valueCoder, windowingStrategy.getWindowFn().windowCoder()));
    }

    /**
     * Returns a unique {@link TupleTag} identifying this {@link PCollectionView}.
     *
     * <p> For internal use only by runner implementors.
     */
    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      return tag;
    }

    /**
     * Returns the {@link WindowingStrategy} of this {@link PCollectionView}, which should
     * be that of the underlying {@link PCollection}.
     *
     * <p> For internal use only by runner implementors.
     */
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
