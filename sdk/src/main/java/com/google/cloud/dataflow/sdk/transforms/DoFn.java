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

import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.reflect.TypeToken;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * The argument to {@link ParDo} providing the code to use to process
 * elements of the input
 * {@link com.google.cloud.dataflow.sdk.values.PCollection}.
 *
 * <p> See {@link ParDo} for more explanation, examples of use, and
 * discussion of constraints on {@code DoFn}s, including their
 * serializability, lack of access to global shared mutable state,
 * requirements for failure tolerance, and benefits of optimization.
 *
 * <p> {@code DoFn}s can be tested in the context of a particular
 * {@code Pipeline} by running that {@code Pipeline} on sample input
 * and then checking its output.  Unit testing of a {@code DoFn},
 * separately from any {@code ParDo} transform or {@code Pipeline},
 * can be done via the {@link DoFnTester} harness.
 *
 * @param <I> the type of the (main) input elements
 * @param <O> the type of the (main) output elements
 */
@SuppressWarnings("serial")
public abstract class DoFn<I, O> implements Serializable {

  /** Information accessible to all methods in this {@code DoFn}. */
  public abstract class Context {

    /**
     * Returns the {@code PipelineOptions} specified with the
     * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner}
     * invoking this {@code DoFn}.  The {@code PipelineOptions} will
     * be the default running via {@link DoFnTester}.
     */
    public abstract PipelineOptions getPipelineOptions();

    /**
     * Returns the value of the side input.
     *
     * @throws IllegalArgumentException if this is not a side input
     * @see ParDo#withSideInputs
     */
    public abstract <T> T sideInput(PCollectionView<T, ?> view);

    /**
     * Adds the given element to the main output {@code PCollection}.
     *
     * <p> If invoked from {@link DoFn#processElement}, the output
     * element will have the same timestamp and be in the same windows
     * as the input element passed to {@link DoFn#processElement}).
     *
     * <p> If invoked from {@link #startBundle} or {@link #finishValue},
     * this will attempt to use the
     * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element. The output element
     * will have a timestamp of negative infinity.
     */
    public abstract void output(O output);

    /**
     * Adds the given element to the main output {@code PCollection},
     * with the given timestamp.
     *
     * <p> If invoked from {@link DoFn#processElement}), the timestamp
     * must not be older than the input element's timestamp minus
     * {@link DoFn#getAllowedTimestampSkew}.  The output element will
     * be in the same windows as the input element.
     *
     * <p> If invoked from {@link #startBundle} or {@link #finishValue},
     * this will attempt to use the
     * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element except for the
     * timestamp.
     */
    public abstract void outputWithTimestamp(O output, Instant timestamp);

    /**
     * Adds the given element to the side output {@code PCollection} with the
     * given tag.
     *
     * <p> The caller of {@code ParDo} uses {@link ParDo#withOutputTags} to
     * specify the tags of side outputs that it consumes. Non-consumed side
     * outputs, e.g., outputs for monitoring purposes only, don't necessarily
     * need to be specified.
     *
     * <p> The output element will have the same timestamp and be in the same
     * windows as the input element passed to {@link DoFn#processElement}).
     *
     * <p> If invoked from {@link #startBundle} or {@link #finishValue},
     * this will attempt to use the
     * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element. The output element
     * will have a timestamp of negative infinity.
     *
     * @throws IllegalArgumentException if the number of outputs exceeds
     * the limit of 1,000 outputs per DoFn
     * @see ParDo#withOutputTags
     */
    public abstract <T> void sideOutput(TupleTag<T> tag, T output);

    /**
     * Adds the given element to the specified side output {@code PCollection},
     * with the given timestamp.
     *
     * <p> If invoked from {@link DoFn#processElement}), the timestamp
     * must not be older than the input element's timestamp minus
     * {@link DoFn#getAllowedTimestampSkew}.  The output element will
     * be in the same windows as the input element.
     *
     * <p> If invoked from {@link #startBundle} or {@link #finishValue},
     * this will attempt to use the
     * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
     * of the input {@code PCollection} to determine what windows the element
     * should be in, throwing an exception if the {@code WindowFn} attempts
     * to access any information about the input element except for the
     * timestamp.
     *
     * @throws IllegalArgumentException if the number of outputs exceeds
     * the limit of 1,000 outputs per DoFn
     * @see ParDo#withOutputTags
     */
    public abstract <T> void sideOutputWithTimestamp(
        TupleTag<T> tag, T output, Instant timestamp);

    /**
     * Returns an aggregator with aggregation logic specified by the CombineFn
     * argument. The name provided should be unique across aggregators created
     * within the containing ParDo transform application.
     *
     * <p> All instances of this DoFn in the containing ParDo
     * transform application should define aggregators consistently,
     * i.e., an aggregator with a given name always specifies the same
     * combiner in all DoFn instances in the containing ParDo
     * transform application.
     *
     * @throws IllegalArgumentException if the given CombineFn is not
     * supported as aggregator's combiner, or if the given name collides
     * with another aggregator or system-provided counter.
     */
    public abstract <AI, AA, AO> Aggregator<AI> createAggregator(
        String name, Combine.CombineFn<? super AI, AA, AO> combiner);

    /**
     * Returns an aggregator with aggregation logic specified by the
     * SerializableFunction argument. The name provided should be unique across
     * aggregators created within the containing ParDo transform application.
     *
     * <p> All instances of this DoFn in the containing ParDo
     * transform application should define aggregators consistently,
     * i.e., an aggregator with a given name always specifies the same
     * combiner in all DoFn instances in the containing ParDo
     * transform application.
     *
     * @throws IllegalArgumentException if the given SerializableFunction is
     * not supported as aggregator's combiner, or if the given name collides
     * with another aggregator or system-provided counter.
     */
    public abstract <AI, AO> Aggregator<AI> createAggregator(
        String name, SerializableFunction<Iterable<AI>, AO> combiner);
  }

  /**
   * Information accessible when running {@link DoFn#processElement}.
   */
  public abstract class ProcessContext extends Context {

    /**
     * Returns the input element to be processed.
     */
    public abstract I element();

    /**
     * Returns this {@code DoFn}'s state associated with the input
     * element's key.  This state can be used by the {@code DoFn} to
     * store whatever information it likes with that key.  Unlike
     * {@code DoFn} instance variables, this state is persistent and
     * can be arbitrarily large; it is more expensive than instance
     * variable state, however.  It is particularly intended for
     * streaming computations.
     *
     * <p> Requires that this {@code DoFn} implements
     * {@link RequiresKeyedState}.
     *
     * <p> Each {@link ParDo} invocation with this {@code DoFn} as an
     * argument will maintain its own {@code KeyedState} maps, one per
     * key.
     *
     * @throws UnsupportedOperationException if this {@link DoFn} does
     * not implement {@link RequiresKeyedState}
     */
    public abstract KeyedState keyedState();

    /**
     * Returns the timestamp of the input element.
     *
     * <p> See {@link com.google.cloud.dataflow.sdk.transforms.windowing.Window}
     * for more information.
     */
    public abstract Instant timestamp();

    /**
     * Returns the set of windows to which the input element has been assigned.
     *
     * <p> See {@link com.google.cloud.dataflow.sdk.transforms.windowing.Window}
     * for more information.
     */
    public abstract Collection<? extends BoundedWindow> windows();
  }

  /**
   * Returns the allowed timestamp skew duration, which is the maximum
   * duration that timestamps can be shifted backward in
   * {@link DoFn.Context#outputWithTimestamp}.
   *
   * The default value is {@code Duration.ZERO}, in which case
   * timestamps can only be shifted forward to future.  For infinite
   * skew, return {@code Duration.millis(Long.MAX_VALUE)}.
   */
  public Duration getAllowedTimestampSkew() {
    return Duration.ZERO;
  }

  /**
   * Interface for signaling that a {@link DoFn} needs to maintain
   * per-key state, accessed via
   * {@link DoFn.ProcessContext#keyedState}.
   *
   * <p> This functionality is experimental and likely to change.
   */
  public interface RequiresKeyedState {}

  /**
   * Interface for interacting with keyed state.
   *
   * <p> This functionality is experimental and likely to change.
   */
  public interface KeyedState {
    /**
     * Updates this {@code KeyedState} in place so that the given tag
     * maps to the given value.
     *
     * @throws IOException if encoding the given value fails
     */
    public <T> void store(CodedTupleTag<T> tag, T value) throws IOException;

    /**
     * Returns the value associated with the given tag in this
     * {@code KeyedState}, or {@code null} if the tag has no asssociated
     * value.
     *
     * <p> See {@link #lookup(List)} to look up multiple tags at
     * once.  It is significantly more efficient to look up multiple
     * tags all at once rather than one at a time.
     *
     * @throws IOException if decoding the requested value fails
     */
    public <T> T lookup(CodedTupleTag<T> tag) throws IOException;

    /**
     * Returns a map from the given tags to the values associated with
     * those tags in this {@code KeyedState}.  A tag will map to null if
     * the tag had no associated value.
     *
     * <p> See {@link #lookup(CodedTupleTag)} to look up a single
     * tag.
     *
     * @throws CoderException if decoding any of the requested values fails
     */
    public CodedTupleTagMap lookup(List<? extends CodedTupleTag<?>> tags) throws IOException;
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Prepares this {@code DoFn} instance for processing a batch of elements.
   *
   * <p> By default, does nothing.
   */
  public void startBundle(Context c) throws Exception {
  }

  /**
   * Processes an input element.
   */
  public abstract void processElement(ProcessContext c) throws Exception;

  /**
   * Finishes processing this batch of elements.  This {@code DoFn}
   * instance will be thrown away after this operation returns.
   *
   * <p> By default, does nothing.
   */
  public void finishBundle(Context c) throws Exception {
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a {@link TypeToken} capturing what is known statically
   * about the input type of this {@code DoFn} instance's most-derived
   * class.
   *
   * <p> See {@link #getOutputTypeToken} for more discussion.
   */
  TypeToken<I> getInputTypeToken() {
    return new TypeToken<I>(getClass()) {};
  }

  /**
   * Returns a {@link TypeToken} capturing what is known statically
   * about the output type of this {@code DoFn} instance's
   * most-derived class.
   *
   * <p> In the normal case of a concrete {@code DoFn} subclass with
   * no generic type parameters of its own (including anonymous inner
   * classes), this will be a complete non-generic type, which is good
   * for choosing a default output {@code Coder<O>} for the output
   * {@code PCollection<O>}.
   */
  TypeToken<O> getOutputTypeToken() {
    return new TypeToken<O>(getClass()) {};
  }
}
