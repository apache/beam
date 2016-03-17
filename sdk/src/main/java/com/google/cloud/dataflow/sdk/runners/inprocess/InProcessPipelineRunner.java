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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.inprocess.GroupByKeyEvaluatorFactory.InProcessGroupByKey;
import com.google.cloud.dataflow.sdk.runners.inprocess.ViewEvaluatorFactory.InProcessCreatePCollectionView;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.View.CreatePCollectionView;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.ImmutableMap;

import org.joda.time.Instant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

/**
 * An In-Memory implementation of the Dataflow Programming Model. Supports Unbounded
 * {@link PCollection PCollections}.
 */
@Experimental
public class InProcessPipelineRunner {
  @SuppressWarnings({"rawtypes", "unused"})
  private static Map<Class<? extends PTransform>, Class<? extends PTransform>>
      defaultTransformOverrides =
          ImmutableMap.<Class<? extends PTransform>, Class<? extends PTransform>>builder()
              .put(GroupByKey.class, InProcessGroupByKey.class)
              .put(CreatePCollectionView.class, InProcessCreatePCollectionView.class)
              .build();

  private static Map<Class<?>, TransformEvaluatorFactory> defaultEvaluatorFactories =
      new ConcurrentHashMap<>();

  /**
   * Register a default transform evaluator.
   */
  public static <TransformT extends PTransform<?, ?>> void registerTransformEvaluatorFactory(
      Class<TransformT> clazz, TransformEvaluatorFactory evaluator) {
    checkArgument(defaultEvaluatorFactories.put(clazz, evaluator) == null,
        "Defining a default factory %s to evaluate Transforms of type %s multiple times", evaluator,
        clazz);
  }

  /**
   * Part of a {@link PCollection}. Elements are output to a bundle, which will cause them to be
   * executed by {@link PTransform PTransforms} that consume the {@link PCollection} this bundle is
   * a part of at a later point. This is an uncommitted bundle and can have elements added to it.
   *
   * @param <T> the type of elements that can be added to this bundle
   */
  public static interface UncommittedBundle<T> {
    /**
     * Returns the PCollection that the elements of this bundle belong to.
     */
    PCollection<T> getPCollection();

    /**
     * Outputs an element to this bundle.
     *
     * @param element the element to add to this bundle
     * @return this bundle
     */
    UncommittedBundle<T> add(WindowedValue<T> element);

    /**
     * Commits this {@link UncommittedBundle}, returning an immutable {@link CommittedBundle}
     * containing all of the elements that were added to it. The {@link #add(WindowedValue)} method
     * will throw an {@link IllegalStateException} if called after a call to commit.
     * @param synchronizedProcessingTime the synchronized processing time at which this bundle was
     *                                   committed
     */
    CommittedBundle<T> commit(Instant synchronizedProcessingTime);
  }

  /**
   * Part of a {@link PCollection}. Elements are output to an {@link UncommittedBundle}, which will
   * eventually committed. Committed elements are executed by the {@link PTransform PTransforms}
   * that consume the {@link PCollection} this bundle is
   * a part of at a later point.
   * @param <T> the type of elements contained within this bundle
   */
  public static interface CommittedBundle<T> {

    /**
     * Returns the PCollection that the elements of this bundle belong to.
     */
    PCollection<T> getPCollection();

    /**
     * Returns weather this bundle is keyed. A bundle that is part of a {@link PCollection} that
     * occurs after a {@link GroupByKey} is keyed by the result of the last {@link GroupByKey}.
     */
    boolean isKeyed();

    /**
     * Returns the (possibly null) key that was output in the most recent {@link GroupByKey} in the
     * execution of this bundle.
     */
    @Nullable Object getKey();

    /**
     * @return an {@link Iterable} containing all of the elements that have been added to this
     *         {@link CommittedBundle}
     */
    Iterable<WindowedValue<T>> getElements();

    /**
     * Returns the processing time output watermark at the time the producing {@link PTransform}
     * committed this bundle. Downstream synchronized processing time watermarks cannot progress
     * past this point before consuming this bundle.
     *
     * <p>This value is no greater than the earliest incomplete processing time or synchronized
     * processing time {@link TimerData timer} at the time this bundle was committed, including any
     * timers that fired to produce this bundle.
     */
    Instant getSynchronizedProcessingOutputWatermark();
  }

  /**
   * A {@link PCollectionViewWriter} is responsible for writing contents of a {@link PCollection} to
   * a storage mechanism that can be read from while constructing a {@link PCollectionView}.
   * @param <ElemT> the type of elements the input {@link PCollection} contains.
   * @param <ViewT> the type of the PCollectionView this writer writes to.
   */
  public static interface PCollectionViewWriter<ElemT, ViewT> {
    void add(Iterable<WindowedValue<ElemT>> values);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  private final InProcessPipelineOptions options;

  public static InProcessPipelineRunner fromOptions(PipelineOptions options) {
    return new InProcessPipelineRunner(options.as(InProcessPipelineOptions.class));
  }

  private InProcessPipelineRunner(InProcessPipelineOptions options) {
    this.options = options;
  }

  /**
   * Returns the {@link PipelineOptions} used to create this {@link InProcessPipelineRunner}.
   */
  public InProcessPipelineOptions getPipelineOptions() {
    return options;
  }
}
