/*
 * Copyright (C) 2014 Google Inc.
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
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValueBase;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Transforms for creating {@link PCollectionView}s from {@link PCollection}s,
 * for consuming the contents of those {@link PCollection}s as side inputs
 * to {@link ParDo} transforms.
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
   * A {@PTransform} that produces a {@link PCollectionView} of a singleton {@link PCollection}
   * yielding the single element it contains.
   *
   * <p> Instantiate via {@link View#asIterable}.
   */
  public static class AsIterable<T> extends PTransform<
      PCollection<T>,
      PCollectionView<Iterable<T>, Iterable<WindowedValue<T>>>> {

    private AsIterable() { }

    @Override
    public PCollectionView<Iterable<T>, Iterable<WindowedValue<T>>> apply(
        PCollection<T> input) {
      return input.apply(
          new CreatePCollectionView<T, Iterable<T>, Iterable<WindowedValue<T>>>(
              new IterablePCollectionView<T>(input.getPipeline())));
    }
  }

  /**
   * A {@PTransform} that produces a {@link PCollectionView} of a singleton {@link PCollection}
   * yielding the single element it contains.
   *
   * <p> Instantiate via {@link View#asIterable}.
   */
  public static class AsSingleton<T>
      extends PTransform<PCollection<T>, PCollectionView<T, WindowedValue<T>>> {

    private AsSingleton() { }

    @Override
    public PCollectionView<T, WindowedValue<T>> apply(PCollection<T> input) {
      return input.apply(
          new CreatePCollectionView<T, T, WindowedValue<T>>(
            new SingletonPCollectionView<T>(input.getPipeline())));
    }

  }


  ////////////////////////////////////////////////////////////////////////////
  // Internal details below

  /**
   * Creates a primitive PCollectionView.
   *
   * <p> For internal use only.
   *
   * @param <R> The type of the elements of the input PCollection
   * @param <T> The type associated with the PCollectionView used as a side input
   * @param <WT> The type associated with a windowed side input from the
   * PCollectionView
   */
  public static class CreatePCollectionView<R, T, WT>
      extends PTransform<PCollection<R>, PCollectionView<T, WT>> {

    private PCollectionView<T, WT> view;

    public CreatePCollectionView(PCollectionView<T, WT> view) {
      this.view = view;
    }

    @Override
    public PCollectionView<T, WT> apply(PCollection<R> input) {
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

            private <R, T, WT> void evaluateTyped(
                CreatePCollectionView<R, T, WT> transform,
                DirectPipelineRunner.EvaluationContext context) {
              List<WindowedValue<R>> elems =
                  context.getPCollectionWindowedValues(transform.getInput());
              context.setPCollectionView(transform.getOutput(), elems);
            }
          });
    }
  }

  private static class SingletonPCollectionView<T>
      extends PCollectionViewBase<T, WindowedValue<T>> {

    public SingletonPCollectionView(Pipeline pipeline) {
      setPipelineInternal(pipeline);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T fromIterableInternal(Iterable<WindowedValue<?>> contents) {
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
      extends PCollectionViewBase<Iterable<T>, Iterable<WindowedValue<T>>> {

    public IterablePCollectionView(Pipeline pipeline) {
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

  private abstract static class PCollectionViewBase<T, WT>
      extends PValueBase
      implements PCollectionView<T, WT> {

    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      return tag;
    }

    private TupleTag<Iterable<WindowedValue<?>>> tag = new TupleTag<>();
  }
}
