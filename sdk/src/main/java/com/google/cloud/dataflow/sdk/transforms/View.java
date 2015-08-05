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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.util.PCollectionViews;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
   * throws {@link java.util.NoSuchElementException} in the consuming
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
    return new AsList<>();
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
  public static class AsList<T> extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    private static final long serialVersionUID = 0;

    private AsList() { }

    @Override
    public void validate(PCollection<T> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }
    }

    @Override
    public PCollectionView<List<T>> apply(PCollection<T> input) {
      return input.apply(Combine.globally(new Concatenate<T>()).asSingletonView());
    }
  }

  /**
   * A {@link PTransform} that produces a {@link PCollectionView} of a singleton
   * {@link PCollection} yielding the single element it contains.
   *
   * <p> Instantiate via {@link View#asIterable}.
   */
  public static class AsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {
    private static final long serialVersionUID = 0;

    private AsIterable() { }

    @Override
    public void validate(PCollection<T> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }
    }

    @Override
    public PCollectionView<Iterable<T>> apply(PCollection<T> input) {
      return input.apply(CreatePCollectionView.<T, Iterable<T>>of(PCollectionViews.iterableView(
          input.getPipeline(), input.getWindowingStrategy(), input.getCoder())));
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
     * Returns whether this transform has a default value.
     */
    public boolean hasDefaultValue() {
      return hasDefault;
    }

    /**
     * Returns the default value of this transform, or null if there isn't one.
     */
    public T defaultValue() {
      return defaultValue;
    }

    /**
     * Default value to return for windows with no value in them.
     */
    public AsSingleton<T> withDefaultValue(T defaultValue) {
      return new AsSingleton<>(defaultValue);
    }

    @Override
    public void validate(PCollection<T> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }
    }

    @Override
    public PCollectionView<T> apply(PCollection<T> input) {
      return input.apply(CreatePCollectionView.<T, T>of(PCollectionViews.singletonView(
          input.getPipeline(),
          input.getWindowingStrategy(),
          hasDefault,
          defaultValue,
          input.getCoder())));
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
    public void validate(PCollection<KV<K, V>> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
      return input.apply(CreatePCollectionView.<KV<K, V>, Map<K, Iterable<V>>>of(
          PCollectionViews.multimapView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder())));
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
    public void validate(PCollection<KV<K, V>> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }
    }

    @Override
    public PCollectionView<Map<K, V>> apply(PCollection<KV<K, V>> input) {
      return input.apply(CreatePCollectionView.<KV<K, V>, Map<K, V>>of(
          PCollectionViews.mapView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder())));
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // Internal details below

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

    private CreatePCollectionView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    public static <ElemT, ViewT> CreatePCollectionView<ElemT, ViewT> of(
        PCollectionView<ViewT> view) {
      return new CreatePCollectionView<>(view);
    }

    @Override
    public PCollectionView<ViewT> apply(PCollection<ElemT> input) {
      return view;
    }

    static {
      DirectPipelineRunner.registerDefaultTransformEvaluator(
          CreatePCollectionView.class,
          new DirectPipelineRunner.TransformEvaluator<CreatePCollectionView>() {
            @SuppressWarnings("rawtypes")
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
   * Combiner that combines {@code T}s into a single {@code List<T>} containing
   * all inputs.
   *
   * <p>For internal use only by {@link View#asList()}, which views a tiny {@link PCollection}
   * that fits in memory as a single {@code List}. For a large {@link PCollection} this is
   * expected to crash!
   *
   * @param <T> the type of elements to concatenate.
   */
  public static class Concatenate<T> extends CombineFn<T, List<T>, List<T>> {
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
}
