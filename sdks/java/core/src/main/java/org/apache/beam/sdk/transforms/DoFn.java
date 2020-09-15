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
package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.TimestampObservingWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The argument to {@link ParDo} providing the code to use to process elements of the input {@link
 * org.apache.beam.sdk.values.PCollection}.
 *
 * <p>See {@link ParDo} for more explanation, examples of use, and discussion of constraints on
 * {@code DoFn}s, including their serializability, lack of access to global shared mutable state,
 * requirements for failure tolerance, and benefits of optimization.
 *
 * <p>{@link DoFn DoFns} can be tested by using {@link TestPipeline}. You can verify their
 * functional correctness in a local test using the {@code DirectRunner} as well as running
 * integration tests with your production runner of choice. Typically, you can generate the input
 * data using {@link Create#of} or other transforms. However, if you need to test the behavior of
 * {@link StartBundle} and {@link FinishBundle} with particular bundle boundaries, you can use
 * {@link TestStream}.
 *
 * <p>Implementations must define a method annotated with {@link ProcessElement} that satisfies the
 * requirements described there. See the {@link ProcessElement} for details.
 *
 * <p>Example usage:
 *
 * <pre><code>
 * {@literal PCollection<String>} lines = ... ;
 * {@literal PCollection<String>} words =
 *     {@literal lines.apply(ParDo.of(new DoFn<String, String>())} {
 *         {@literal @ProcessElement}
 *          public void processElement({@literal @}Element String element, BoundedWindow window) {
 *            ...
 *          }}));
 * </code></pre>
 *
 * @param <InputT> the type of the (main) input elements
 * @param <OutputT> the type of the (main) output elements
 */
public abstract class DoFn<InputT extends @Nullable Object, OutputT extends @Nullable Object>
    implements Serializable, HasDisplayData {
  /** Information accessible while within the {@link StartBundle} method. */
  @SuppressWarnings("ClassCanBeStatic") // Converting class to static is an API change.
  public abstract class StartBundleContext {
    /**
     * Returns the {@code PipelineOptions} specified with the {@link
     * org.apache.beam.sdk.PipelineRunner} invoking this {@code DoFn}.
     */
    public abstract PipelineOptions getPipelineOptions();
  }

  /** Information accessible while within the {@link FinishBundle} method. */
  public abstract class FinishBundleContext {
    /**
     * Returns the {@code PipelineOptions} specified with the {@link
     * org.apache.beam.sdk.PipelineRunner} invoking this {@code DoFn}.
     */
    public abstract PipelineOptions getPipelineOptions();

    /**
     * Adds the given element to the main output {@code PCollection} at the given timestamp in the
     * given window.
     *
     * <p>Once passed to {@code output} the element should not be modified in any way.
     *
     * <p><i>Note:</i> A splittable {@link DoFn} is not allowed to output from the {@link
     * FinishBundle} method.
     */
    public abstract void output(OutputT output, Instant timestamp, BoundedWindow window);

    /**
     * Adds the given element to the output {@code PCollection} with the given tag at the given
     * timestamp in the given window.
     *
     * <p>Once passed to {@code output} the element should not be modified in any way.
     *
     * <p><i>Note:</i> A splittable {@link DoFn} is not allowed to output from the {@link
     * FinishBundle} method.
     */
    public abstract <T> void output(
        TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window);
  }

  /**
   * Information accessible to all methods in this {@link DoFn} where the context is in some window.
   */
  public abstract class WindowedContext {
    /**
     * Returns the {@code PipelineOptions} specified with the {@link
     * org.apache.beam.sdk.PipelineRunner} invoking this {@code DoFn}.
     */
    public abstract PipelineOptions getPipelineOptions();

    /**
     * Adds the given element to the main output {@code PCollection}.
     *
     * <p>Once passed to {@code output} the element should not be modified in any way.
     *
     * <p>If invoked from {@link ProcessElement}, the output element will have the same timestamp
     * and be in the same windows as the input element passed to the method annotated with
     * {@code @ProcessElement}.
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle}, this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn} of the input {@code PCollection} to
     * determine what windows the element should be in, throwing an exception if the {@code
     * WindowFn} attempts to access any information about the input element. The output element will
     * have a timestamp of negative infinity.
     *
     * <p><i>Note:</i> A splittable {@link DoFn} is not allowed to output from {@link StartBundle}
     * or {@link FinishBundle} methods.
     */
    public abstract void output(OutputT output);

    /**
     * Adds the given element to the main output {@code PCollection}, with the given timestamp.
     *
     * <p>Once passed to {@code outputWithTimestamp} the element should not be modified in any way.
     *
     * <p>If invoked from {@link ProcessElement}), the timestamp must not be older than the input
     * element's timestamp minus {@link DoFn#getAllowedTimestampSkew}. The output element will be in
     * the same windows as the input element.
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle}, this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn} of the input {@code PCollection} to
     * determine what windows the element should be in, throwing an exception if the {@code
     * WindowFn} attempts to access any information about the input element except for the
     * timestamp.
     *
     * <p><i>Note:</i> A splittable {@link DoFn} is not allowed to output from {@link StartBundle}
     * or {@link FinishBundle} methods.
     */
    public abstract void outputWithTimestamp(OutputT output, Instant timestamp);

    /**
     * Adds the given element to the output {@code PCollection} with the given tag.
     *
     * <p>Once passed to {@code output} the element should not be modified in any way.
     *
     * <p>The caller of {@code ParDo} uses {@link ParDo.SingleOutput#withOutputTags} to specify the
     * tags of outputs that it consumes. Non-consumed outputs, e.g., outputs for monitoring purposes
     * only, don't necessarily need to be specified.
     *
     * <p>The output element will have the same timestamp and be in the same windows as the input
     * element passed to {@link ProcessElement}).
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle}, this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn} of the input {@code PCollection} to
     * determine what windows the element should be in, throwing an exception if the {@code
     * WindowFn} attempts to access any information about the input element. The output element will
     * have a timestamp of negative infinity.
     *
     * <p><i>Note:</i> A splittable {@link DoFn} is not allowed to output from {@link StartBundle}
     * or {@link FinishBundle} methods.
     *
     * @see ParDo.SingleOutput#withOutputTags
     */
    public abstract <T> void output(TupleTag<T> tag, T output);

    /**
     * Adds the given element to the specified output {@code PCollection}, with the given timestamp.
     *
     * <p>Once passed to {@code outputWithTimestamp} the element should not be modified in any way.
     *
     * <p>If invoked from {@link ProcessElement}), the timestamp must not be older than the input
     * element's timestamp minus {@link DoFn#getAllowedTimestampSkew}. The output element will be in
     * the same windows as the input element.
     *
     * <p>If invoked from {@link StartBundle} or {@link FinishBundle}, this will attempt to use the
     * {@link org.apache.beam.sdk.transforms.windowing.WindowFn} of the input {@code PCollection} to
     * determine what windows the element should be in, throwing an exception if the {@code
     * WindowFn} attempts to access any information about the input element except for the
     * timestamp.
     *
     * <p><i>Note:</i> A splittable {@link DoFn} is not allowed to output from {@link StartBundle}
     * or {@link FinishBundle} methods.
     *
     * @see ParDo.SingleOutput#withOutputTags
     */
    public abstract <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp);
  }

  /** Information accessible when running a {@link DoFn.ProcessElement} method. */
  public abstract class ProcessContext extends WindowedContext {

    /**
     * Returns the input element to be processed.
     *
     * <p>The element will not be changed -- it is safe to cache, etc. without copying.
     * Implementation of {@link DoFn.ProcessElement} method should not mutate the element.
     */
    public abstract InputT element();

    /**
     * Returns the value of the side input.
     *
     * @throws IllegalArgumentException if this is not a side input
     * @see ParDo.SingleOutput#withSideInputs
     */
    public abstract <T> T sideInput(PCollectionView<T> view);

    /**
     * Returns the timestamp of the input element.
     *
     * <p>See {@link Window} for more information.
     */
    public abstract Instant timestamp();

    /**
     * Returns information about the pane within this window into which the input element has been
     * assigned.
     *
     * <p>Generally all data is in a single, uninteresting pane unless custom triggering and/or late
     * data has been explicitly requested. See {@link Window} for more information.
     */
    public abstract PaneInfo pane();
  }

  /** Information accessible when running a {@link DoFn.OnTimer} method. */
  @Experimental(Kind.TIMERS)
  public abstract class OnTimerContext extends WindowedContext {

    /** Returns the output timestamp of the current timer. */
    public abstract Instant timestamp();

    /** Returns the firing timestamp of the current timer. */
    public abstract Instant fireTimestamp();

    /** Returns the window in which the timer is firing. */
    public abstract BoundedWindow window();

    /** Returns the time domain of the current timer. */
    public abstract TimeDomain timeDomain();
  }

  public abstract class OnWindowExpirationContext extends WindowedContext {

    /** Returns the window in which the window expiration is firing. */
    public abstract BoundedWindow window();
  }

  /**
   * Returns the allowed timestamp skew duration, which is the maximum duration that timestamps can
   * be shifted backward in {@link WindowedContext#outputWithTimestamp}.
   *
   * <p>The default value is {@code Duration.ZERO}, in which case timestamps can only be shifted
   * forward to future. For infinite skew, return {@code Duration.millis(Long.MAX_VALUE)}.
   *
   * @deprecated This method permits a {@link DoFn} to emit elements behind the watermark. These
   *     elements are considered late, and if behind the {@link Window#withAllowedLateness(Duration)
   *     allowed lateness} of a downstream {@link PCollection} may be silently dropped. See
   *     https://issues.apache.org/jira/browse/BEAM-644 for details on a replacement.
   */
  @Deprecated
  public Duration getAllowedTimestampSkew() {
    return Duration.ZERO;
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically about the input type of
   * this {@code DoFn} instance's most-derived class.
   *
   * <p>See {@link #getOutputTypeDescriptor} for more discussion.
   */
  public TypeDescriptor<InputT> getInputTypeDescriptor() {
    return new TypeDescriptor<InputT>(getClass()) {};
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically about the output type of
   * this {@code DoFn} instance's most-derived class.
   *
   * <p>In the normal case of a concrete {@code DoFn} subclass with no generic type parameters of
   * its own (including anonymous inner classes), this will be a complete non-generic type, which is
   * good for choosing a default output {@code Coder<O>} for the output {@code PCollection<O>}.
   */
  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
    return new TypeDescriptor<OutputT>(getClass()) {};
  }

  /** Receives values of the given type. */
  public interface OutputReceiver<T> {
    void output(T output);

    void outputWithTimestamp(T output, Instant timestamp);
  }

  /** Receives tagged output for a multi-output function. */
  public interface MultiOutputReceiver {
    /** Returns an {@link OutputReceiver} for the given tag. * */
    <T> OutputReceiver<T> get(TupleTag<T> tag);

    /**
     * Returns a {@link OutputReceiver} for publishing {@link Row} objects to the given tag.
     *
     * <p>The {@link PCollection} representing this tag must have a schema registered in order to
     * call this function.
     */
    @Experimental(Kind.SCHEMAS)
    <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag);
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Annotation for declaring and dereferencing state cells.
   *
   * <p>To declare a state cell, create a field of type {@link StateSpec} annotated with a {@link
   * StateId}. To use the cell during processing, add a parameter of the appropriate {@link State}
   * subclass to your {@link ProcessElement @ProcessElement} or {@link OnTimer @OnTimer} method, and
   * annotate it with {@link StateId}. See the following code for an example:
   *
   * <pre><code>{@literal new DoFn<KV<Key, Foo>, Baz>()} {
   *
   *  {@literal @StateId("my-state-id")}
   *  {@literal private final StateSpec<ValueState<MyState>>} myStateSpec =
   *       StateSpecs.value(new MyStateCoder());
   *
   *  {@literal @ProcessElement}
   *   public void processElement(
   *       {@literal @Element InputT element},
   *      {@literal @StateId("my-state-id") ValueState<MyState> myState}) {
   *     myState.read();
   *     myState.write(...);
   *   }
   * }
   * </code></pre>
   *
   * <p>State is subject to the following validity conditions:
   *
   * <ul>
   *   <li>Each state ID must be declared at most once.
   *   <li>Any state referenced in a parameter must be declared with the same state type.
   *   <li>State declarations must be final.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.PARAMETER})
  @Experimental(Kind.STATE)
  public @interface StateId {
    /** The state ID. */
    String value();
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Annotation for declaring that a state parameter is always fetched.
   *
   * <p>A DoFn might not fetch a state value on every element, and for that reason runners may
   * choose to defer fetching state until read() is called. Annotating a state argument with this
   * parameter provides a hint to the runner that the state is always fetched. This may cause the
   * runner to prefetch all the state before calling the processElement or processTimer method,
   * improving performance. This is a performance-only hint - it does not change semantics. See the
   * following code for an example:
   *
   * <pre><code>{@literal new DoFn<KV<Key, Foo>, Baz>()} {
   *
   *  {@literal @StateId("my-state-id")}
   *  {@literal private final StateSpec<ValueState<MyState>>} myStateSpec =
   *       StateSpecs.value(new MyStateCoder());
   *
   *  {@literal @ProcessElement}
   *   public void processElement(
   *       {@literal @Element InputT element},
   *      {@literal @AlwaysFetched @StateId("my-state-id") ValueState<MyState> myState}) {
   *     myState.read();
   *     myState.write(...);
   *   }
   * }
   * </code></pre>
   *
   * <p>This can only be used on state objects that implement {@link
   * org.apache.beam.sdk.state.ReadableState}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.PARAMETER})
  @Experimental(Kind.STATE)
  public @interface AlwaysFetched {}

  /**
   * Annotation for declaring and dereferencing timers.
   *
   * <p>To declare a timer, create a field of type {@link TimerSpec} annotated with a {@link
   * TimerId}. To use the cell during processing, add a parameter of the type {@link Timer} to your
   * {@link ProcessElement @ProcessElement} or {@link OnTimer @OnTimer} method, and annotate it with
   * {@link TimerId}. See the following code for an example:
   *
   * <pre><code>{@literal new DoFn<KV<Key, Foo>, Baz>()} {
   *   {@literal @TimerId("my-timer-id")}
   *    private final TimerSpec myTimer = TimerSpecs.timerForDomain(TimeDomain.EVENT_TIME);
   *
   *   {@literal @ProcessElement}
   *    public void processElement(
   *       {@literal @Element InputT element},
   *       {@literal @TimerId("my-timer-id") Timer myTimer}) {
   *      myTimer.offset(Duration.standardSeconds(...)).setRelative();
   *    }
   *
   *   {@literal @OnTimer("my-timer-id")}
   *    public void onMyTimer() {
   *      ...
   *    }
   * }</code></pre>
   *
   * <p>Timers are subject to the following validity conditions:
   *
   * <ul>
   *   <li>Each timer must have a distinct id.
   *   <li>Any timer referenced in a parameter must be declared.
   *   <li>Timer declarations must be final.
   *   <li>All declared timers must have a corresponding callback annotated with {@link
   *       OnTimer @OnTimer}.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.PARAMETER})
  @Experimental(Kind.TIMERS)
  public @interface TimerId {
    /** The timer ID. */
    String value() default "";
  }

  /** Parameter annotation for the TimerMap for a {@link ProcessElement} method. */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.PARAMETER})
  @Experimental(Kind.TIMERS)
  public @interface TimerFamily {
    /** The TimerMap tag ID. */
    String value();
  }

  /**
   * Parameter annotation for dereferencing input element key in {@link
   * org.apache.beam.sdk.values.KV} pair.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface Key {}

  /** Annotation for specifying specific fields that are accessed in a Schema PCollection. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.PARAMETER})
  @Experimental(Kind.SCHEMAS)
  public @interface FieldAccess {
    String value();
  }

  /**
   * Annotation for registering a callback for a timer.
   *
   * <p>See the javadoc for {@link TimerId} for use in a full example.
   *
   * <p>The method annotated with {@code @OnTimer} may have parameters according to the same logic
   * as {@link ProcessElement}, but limited to the {@link BoundedWindow}, {@link State} subclasses,
   * and {@link Timer}. State and timer parameters must be annotated with their {@link StateId} and
   * {@link TimerId} respectively.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.TIMERS)
  public @interface OnTimer {
    /** The timer ID. */
    String value();
  }

  /**
   * Annotation for registering a callback for a timerFamily.
   *
   * <p>See the javadoc for {@link TimerFamily} for use in a full example.
   *
   * <p>The method annotated with {@code @OnTimerFamily} may have parameters according to the same
   * logic as {@link ProcessElement}, but limited to the {@link BoundedWindow}, {@link State}
   * subclasses, and {@link org.apache.beam.sdk.state.TimerMap}. State and timer parameters must be
   * annotated with their {@link StateId} and {@link TimerId} respectively.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.TIMERS)
  public @interface OnTimerFamily {
    /** The timer ID. */
    String value();
  }

  /**
   * Annotation for the method to use for performing actions on window expiration. For example,
   * users can use this annotation to write a method that extracts a value saved in a state before
   * it gets garbage collected on window expiration.
   *
   * <p>The method annotated with {@code @OnWindowExpiration} may have parameters according to the
   * same logic as {@link OnTimer}. See the following code for an example:
   *
   * <pre><code>{@literal new DoFn<KV<Key, Foo>, Baz>()} {
   *
   *   {@literal @ProcessElement}
   *    public void processElement(ProcessContext c) {
   *    }
   *
   *   {@literal @OnWindowExpiration}
   *    public void onWindowExpiration() {
   *      ...
   *    }
   * }</code></pre>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.STATE)
  public @interface OnWindowExpiration {}

  /**
   * Annotation for the method to use to prepare an instance for processing bundles of elements.
   *
   * <p>This is a good place to initialize transient in-memory resources, such as network
   * connections. The resources can then be disposed in {@link Teardown}.
   *
   * <p>This is <b>not</b> a good place to perform external side-effects that later need cleanup,
   * e.g. creating temporary files on distributed filesystems, starting VMs, or initiating data
   * export jobs. Such logic must be instead implemented purely via {@link StartBundle}, {@link
   * ProcessElement} and {@link FinishBundle} methods, references to the objects requiring cleanup
   * must be passed as {@link PCollection} elements, and they must be cleaned up via regular Beam
   * transforms, e.g. see the {@link Wait} transform.
   *
   * <p>The method annotated with this must satisfy the following constraints:
   *
   * <ul>
   *   <li>It must have zero arguments.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface Setup {}

  /**
   * Annotation for the method to use to prepare an instance for processing a batch of elements. The
   * method annotated with this must satisfy the following constraints:
   *
   * <ul>
   *   <li>If one of the parameters is of type {@link DoFn.StartBundleContext}, then it will be
   *       passed a context object for the current execution.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   *   <li>If one of the parameters is of type {@link BundleFinalizer}, then it will be passed a
   *       mechanism to register a callback that will be invoked after the runner successfully
   *       commits the output of this bundle. See <a
   *       href="https://s.apache.org/beam-finalizing-bundles">Apache Beam Portability API: How to
   *       Finalize Bundles</a> for further details.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface StartBundle {}

  /**
   * Annotation for the method to use for processing elements. A subclass of {@link DoFn} must have
   * a method with this annotation.
   *
   * <p>If any of the arguments is a {@link RestrictionTracker} then see the specifications below
   * about splittable {@link DoFn}, otherwise this method must satisfy the following constraints:
   *
   * <ul>
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed. The argument type must match the input type
   *       of this DoFn exactly, or both types must have equivalent schemas registered.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   *   <li>If one of the parameters is of type {@link OutputReceiver}, then it will be passed an
   *       output receiver for outputting elements to the default output.
   *   <li>If one of the parameters is of type {@link MultiOutputReceiver}, then it will be passed
   *       an output receiver for outputting to multiple tagged outputs.
   *   <li>If one of the parameters is of type {@link BundleFinalizer}, then it will be passed a
   *       mechanism to register a callback that will be invoked after the runner successfully
   *       commits the output of this bundle. See <a
   *       href="https://s.apache.org/beam-finalizing-bundles">Apache Beam Portability API: How to
   *       Finalize Bundles</a> for further details.
   *   <li>It must return {@code void}.
   * </ul>
   *
   * <h2>Splittable DoFn's</h2>
   *
   * <p>A {@link DoFn} is <i>splittable</i> if its {@link ProcessElement} method has a parameter
   * whose type is of {@link RestrictionTracker}. This is an advanced feature and an overwhelming
   * majority of users will never need to write a splittable {@link DoFn}.
   *
   * <p>Not all runners support Splittable DoFn. See the <a
   * href="https://beam.apache.org/documentation/runners/capability-matrix/">capability matrix</a>.
   *
   * <p>See <a href="https://s.apache.org/splittable-do-fn">the proposal</a> for an overview of the
   * involved concepts (<i>splittable DoFn</i>, <i>restriction</i>, <i>restriction tracker</i>).
   *
   * <p>A splittable {@link DoFn} must obey the following constraints:
   *
   * <ul>
   *   <li>The type of restrictions used by all of these methods must be the same.
   *   <li>It <i>must</i> define a {@link GetInitialRestriction} method.
   *   <li>It <i>may</i> define a {@link GetSize} method or ensure that the {@link
   *       RestrictionTracker} implements {@link RestrictionTracker.HasProgress}. Poor auto-scaling
   *       of workers and/or splitting may result if size or progress is an inaccurate
   *       representation of work. See {@link GetSize} and {@link RestrictionTracker.HasProgress}
   *       for further details.
   *   <li>It <i>should</i> define a {@link SplitRestriction} method. This method enables runners to
   *       perform bulk splitting initially allowing for a rapid increase in parallelism. See {@link
   *       RestrictionTracker#trySplit} for details about splitting when the current element and
   *       restriction are actively being processed.
   *   <li>It <i>may</i> define a {@link TruncateRestriction} method to choose how to truncate a
   *       restriction such that it represents a finite amount of work when the pipeline is
   *       draining. See {@link TruncateRestriction} and {@link RestrictionTracker#isBounded} for
   *       additional details.
   *   <li>It <i>may</i> define a {@link NewTracker} method returning a subtype of {@code
   *       RestrictionTracker<R>} where {@code R} is the restriction type returned by {@link
   *       GetInitialRestriction}. This method is optional only if the restriction type returned by
   *       {@link GetInitialRestriction} implements {@link HasDefaultTracker}.
   *   <li>It <i>may</i> define a {@link GetRestrictionCoder} method.
   *   <li>It <i>may</i> define a {@link GetInitialWatermarkEstimatorState} method. If none is
   *       defined then the watermark estimator state is of type {@link Void}.
   *   <li>It <i>may</i> define a {@link GetWatermarkEstimatorStateCoder} method.
   *   <li>It <i>may</i> define a {@link NewWatermarkEstimator} method returning a subtype of {@code
   *       WatermarkEstimator<W>} where {@code W} is the watermark estimator state type returned by
   *       {@link GetInitialWatermarkEstimatorState}. This method is optional only if {@link
   *       GetInitialWatermarkEstimatorState} has not been defined or {@code W} implements {@link
   *       HasDefaultWatermarkEstimator}.
   *   <li>The {@link DoFn} itself <i>may</i> be annotated with {@link BoundedPerElement} or {@link
   *       UnboundedPerElement}, but not both at the same time. If it's not annotated with either of
   *       these, it's assumed to be {@link BoundedPerElement} if its {@link ProcessElement} method
   *       returns {@code void} and {@link UnboundedPerElement} if it returns a {@link
   *       ProcessContinuation}.
   *   <li>Timers and state must not be used.
   * </ul>
   *
   * <p>If this DoFn is splittable, this method must satisfy the following constraints:
   *
   * <ul>
   *   <li>One of its arguments must be a {@link RestrictionTracker}. The argument must be of the
   *       exact type {@code RestrictionTracker<RestrictionT, PositionT>}.
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed. The argument type must match the input type
   *       of this DoFn exactly, or both types must have equivalent schemas registered.
   *   <li>If one of its arguments is tagged with the {@link Restriction} annotation, then it will
   *       be passed the current restriction being processed; the argument must be of type {@code
   *       RestrictionT}.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is of the type {@link WatermarkEstimator}, then it will be passed
   *       the watermark estimator.
   *   <li>If one of its arguments is of the type {@link ManualWatermarkEstimator}, then it will be
   *       passed a watermark estimator that can be updated manually. This parameter can only be
   *       supplied if the method annotated with {@link GetInitialWatermarkEstimatorState} returns a
   *       sub-type of {@link ManualWatermarkEstimator}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   *   <li>If one of the parameters is of type {@link OutputReceiver}, then it will be passed an
   *       output receiver for outputting elements to the default output.
   *   <li>If one of the parameters is of type {@link MultiOutputReceiver}, then it will be passed
   *       an output receiver for outputting to multiple tagged outputs.
   *   <li>If one of the parameters is of type {@link BundleFinalizer}, then it will be passed a
   *       mechanism to register a callback that will be invoked after the runner successfully
   *       commits the output of this bundle. See <a
   *       href="https://s.apache.org/beam-finalizing-bundles">Apache Beam Portability API: How to
   *       Finalize Bundles</a> for further details.
   *   <li>May return a {@link ProcessContinuation} to indicate whether there is more work to be
   *       done for the current element, otherwise must return {@code void}.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface ProcessElement {}

  /**
   * Parameter annotation for the input element for {@link ProcessElement}, {@link
   * GetInitialRestriction}, {@link GetSize}, {@link SplitRestriction}, {@link
   * GetInitialWatermarkEstimatorState}, {@link NewWatermarkEstimator}, and {@link NewTracker}
   * methods.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface Element {}

  /**
   * Parameter annotation for the restriction for {@link GetSize}, {@link SplitRestriction}, {@link
   * GetInitialWatermarkEstimatorState}, {@link NewWatermarkEstimator}, and {@link NewTracker}
   * methods. Must match the return type used on the method annotated with {@link
   * GetInitialRestriction}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface Restriction {}

  /**
   * Parameter annotation for the input element timestamp for {@link ProcessElement}, {@link
   * GetInitialRestriction}, {@link GetSize}, {@link SplitRestriction}, {@link
   * GetInitialWatermarkEstimatorState}, {@link NewWatermarkEstimator}, and {@link NewTracker}
   * methods.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface Timestamp {}

  /** Parameter annotation for the SideInput for a {@link ProcessElement} method. */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface SideInput {
    /** The SideInput tag ID. */
    String value();
  }
  /**
   * <b><i>Experimental - no backwards compatibility guarantees. The exact name or usage of this
   * feature may change.</i></b>
   *
   * <p>Annotation that may be added to a {@link ProcessElement}, {@link OnTimer}, or {@link
   * OnWindowExpiration} method to indicate that the runner must ensure that the observable contents
   * of the input {@link PCollection} or mutable state must be stable upon retries.
   *
   * <p>This is important for sinks, which must ensure exactly-once semantics when writing to a
   * storage medium outside of your pipeline. A general pattern for a basic sink is to write a
   * {@link DoFn} that can perform an idempotent write, and annotate that it requires stable input.
   * Combined, these allow the write to be freely retried until success.
   *
   * <p>An example of an unstable input would be anything computed using nondeterministic logic. In
   * Beam, any user-defined function is permitted to be nondeterministic, and any {@link
   * PCollection} is permitted to be recomputed in any manner.
   */
  @Documented
  @Experimental
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface RequiresStableInput {}

  /**
   * <b><i>Experimental - no backwards compatibility guarantees. The exact name or usage of this
   * feature may change.</i></b>
   *
   * <p>Annotation that may be added to a {@link ProcessElement} method to indicate that the runner
   * must ensure that the observable contents of the input {@link PCollection} is sorted by time, in
   * ascending order. The time ordering is defined by element's timestamp, ordering of elements with
   * equal timestamps is not defined.
   *
   * <p>Note that this annotation makes sense only for stateful {@code ParDo}s, because outcome of
   * stateless functions cannot depend on the ordering.
   *
   * <p>This annotation respects specified <i>allowedLateness</i> defined in {@link
   * WindowingStrategy}. All data is emitted <b>after</b> input watermark passes element's timestamp
   * + allowedLateness. Output watermark is hold, so that the emitted data is not emitted as late
   * data.
   *
   * <p>The ordering requirements implies that all data that arrives later than the allowed lateness
   * will have to be dropped. This might change in the future with introduction of retractions.
   */
  @Documented
  @Experimental
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface RequiresTimeSortedInput {}

  /**
   * Annotation for the method to use to finish processing a batch of elements. The method annotated
   * with this must satisfy the following constraints:
   *
   * <ul>
   *   <li>If one of the parameters is of type {@link DoFn.FinishBundleContext}, then it will be
   *       passed a context object for the current execution.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   *   <li>If one of the parameters is of type {@link BundleFinalizer}, then it will be passed a
   *       mechanism to register a callback that will be invoked after the runner successfully
   *       commits the output of this bundle. See <a
   *       href="https://s.apache.org/beam-finalizing-bundles">Apache Beam Portability API: How to
   *       Finalize Bundles</a> for further details.
   *   <li>TODO(BEAM-1287): Add support for an {@link OutputReceiver} and {@link
   *       MultiOutputReceiver} that can output to a window.
   * </ul>
   *
   * <p>Note that {@link FinishBundle @FinishBundle} is invoked before the runner commits the output
   * while {@link BundleFinalizer.Callback bundle finalizer callbacks} are invoked after the runner
   * has committed the output of a successful bundle.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface FinishBundle {}

  /**
   * Annotation for the method to use to clean up this instance before it is discarded. No other
   * method will be called after a call to the annotated method is made.
   *
   * <p>A runner will do its best to call this method on any given instance to prevent leaks of
   * transient resources, however, there may be situations where this is impossible (e.g. process
   * crash, hardware failure, etc.) or unnecessary (e.g. the pipeline is shutting down and the
   * process is about to be killed anyway, so all transient resources will be released automatically
   * by the OS). In these cases, the call may not happen. It will also not be retried, because in
   * such situations the DoFn instance no longer exists, so there's no instance to retry it on.
   *
   * <p>Thus, all work that depends on input elements, and all externally important side effects,
   * must be performed in the {@link ProcessElement} or {@link FinishBundle} methods.
   *
   * <p>Example things that are a good idea to do in this method:
   *
   * <ul>
   *   <li>Close a network connection that was opened in {@link Setup}
   *   <li>Shut down a helper process that was started in {@link Setup}
   * </ul>
   *
   * <p>Example things that MUST NOT be done in this method:
   *
   * <ul>
   *   <li>Flushing a batch of buffered records to a database: this must be done in {@link
   *       FinishBundle}.
   *   <li>Deleting temporary files on a distributed filesystem: this must be done using the
   *       pipeline structure, e.g. using the {@link Wait} transform.
   * </ul>
   *
   * <p>The method annotated with this must satisfy the following constraint:
   *
   * <ul>
   *   <li>It must have zero arguments.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface Teardown {}

  /**
   * Annotation for the method that maps an element to an initial restriction for a <a
   * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>Signature: {@code RestrictionT getInitialRestriction(<arguments>);}
   *
   * <p>This method must satisfy the following constraints:
   *
   * <ul>
   *   <li>The return type {@code RestrictionT} defines the restriction type used within this
   *       splittable DoFn. All other methods that use a {@link Restriction @Restriction} parameter
   *       must use the same type that is used here. It is suggested to use as narrow of a return
   *       type definition as possible (for example prefer to use a square type over a shape type as
   *       a square is a type of a shape).
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed; the argument must be of type {@code InputT}.
   *       Note that automatic conversion of {@link Row}s and {@link FieldAccess} parameters are
   *       currently unsupported.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface GetInitialRestriction {}

  /**
   * Annotation for the method that returns the corresponding size for an element and restriction
   * pair.
   *
   * <p>Signature: {@code double getSize(<arguments>);}
   *
   * <p>This method must satisfy the following constraints:
   *
   * <ul>
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed; the argument must be of type {@code InputT}.
   *       Note that automatic conversion of {@link Row}s and {@link FieldAccess} parameters are
   *       currently unsupported.
   *   <li>If one of its arguments is tagged with the {@link Restriction} annotation, then it will
   *       be passed the current restriction being processed; the argument must be of type {@code
   *       RestrictionT}.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a {@link RestrictionTracker}, then it will be passed a tracker
   *       that is initialized for the current {@link Restriction}. The argument must be of the
   *       exact type {@code RestrictionTracker<RestrictionT, PositionT>}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   * </ul>
   *
   * <p>Returns a double representing the size of the current element and restriction.
   *
   * <p>Splittable {@link DoFn}s should only provide this method if the default {@link
   * RestrictionTracker.HasProgress} implementation within the {@link RestrictionTracker} is an
   * inaccurate representation of known work.
   *
   * <p>It is up to each splittable {@DoFn} to convert between their natural representation of
   * outstanding work and this representation. For example:
   *
   * <ul>
   *   <li>Block based file source (e.g. Avro): The number of bytes that will be read from the file.
   *   <li>Pull based queue based source (e.g. Pubsub): The local/global size available in number of
   *       messages or number of {@code message bytes} that have not been processed.
   *   <li>Key range based source (e.g. Shuffle, Bigtable, ...): Typically {@code 1.0} unless
   *       additional details such as the number of bytes for keys and values is known for the key
   *       range.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface GetSize {}

  /**
   * Annotation for the method that returns the coder to use for the restriction of a <a
   * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>If not defined, a coder will be inferred using standard coder inference rules and the
   * pipeline's {@link Pipeline#getCoderRegistry coder registry}.
   *
   * <p>This method will be called only at pipeline construction time.
   *
   * <p>Signature: {@code Coder<RestrictionT> getRestrictionCoder();}
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface GetRestrictionCoder {}

  /**
   * Annotation for the method that splits restriction of a <a
   * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} into multiple parts to
   * be processed in parallel.
   *
   * <p>This method is used to perform bulk splitting while a restriction is not actively being
   * processed while {@link RestrictionTracker#trySplit} is used to perform splitting during
   * processing.
   *
   * <p>Signature: {@code void splitRestriction(<arguments>);}
   *
   * <p>This method must satisfy the following constraints:
   *
   * <ul>
   *   <li>If one of the arguments is of type {@link OutputReceiver}, then it will be passed an
   *       output receiver for outputting the splits. All splits must be output through this
   *       parameter.
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed; the argument must be of type {@code InputT}.
   *       Note that automatic conversion of {@link Row}s and {@link FieldAccess} parameters are
   *       currently unsupported.
   *   <li>If one of its arguments is tagged with the {@link Restriction} annotation, then it will
   *       be passed the current restriction being processed; the argument must be of type {@code
   *       RestrictionT}.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a {@link RestrictionTracker}, then it will be passed a tracker
   *       that is initialized for the current {@link Restriction}. The argument must be of the
   *       exact type {@code RestrictionTracker<RestrictionT, PositionT>}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface SplitRestriction {}

  /**
   * Annotation for the method that truncates the restriction of a <a
   * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} into a bounded one.
   * This method is invoked when a pipeline is being <a
   * href="https://docs.google.com/document/d/1NExwHlj-2q2WUGhSO4jTu8XGhDPmm3cllSN8IMmWci8/edit#">drained</a>.
   *
   * <p>This method is used to perform truncation of the restriction while it is not actively being
   * processed.
   *
   * <p>Signature: {@code @Nullable TruncateResult<RestrictionT> truncateRestriction(<arguments>);}
   *
   * <p>This method must satisfy the following constraints:
   *
   * <ul>
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed; the argument must be of type {@code InputT}.
   *       Note that automatic conversion of {@link Row}s and {@link FieldAccess} parameters are
   *       currently unsupported.
   *   <li>If one of its arguments is tagged with the {@link Restriction} annotation, then it will
   *       be passed the current restriction being processed; the argument must be of type {@code
   *       RestrictionT}.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a {@link RestrictionTracker}, then it will be passed a tracker
   *       that is initialized for the current {@link Restriction}. The argument must be of the
   *       exact type {@code RestrictionTracker<RestrictionT, PositionT>}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   * </ul>
   *
   * <p>Returns a truncated restriction representing a bounded amount of work that must be processed
   * before the pipeline can be drained or {@code null} if no work is necessary.
   *
   * <p>The default behavior when a pipeline is being drained is that {@link
   * org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.IsBounded#BOUNDED}
   * restrictions process entirely while {@link
   * org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.IsBounded#UNBOUNDED}
   * restrictions process till a checkpoint is possible. Splittable {@link DoFn}s should only
   * provide this method if they want to change this default behavior.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface TruncateRestriction {}

  /**
   * Annotation for the method that creates a new {@link RestrictionTracker} for the restriction of
   * a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>Signature: {@code MyRestrictionTracker newTracker(<optional arguments>);}
   *
   * <p>This method must satisfy the following constraints:
   *
   * <ul>
   *   <li>The return type must be a subtype of {@code RestrictionTracker<RestrictionT, PositionT>}.
   *       It is suggested to use as narrow of a return type definition as possible (for example
   *       prefer to use a square type over a shape type as a square is a type of a shape).
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed; the argument must be of type {@code InputT}.
   *       Note that automatic conversion of {@link Row}s and {@link FieldAccess} parameters are
   *       currently unsupported.
   *   <li>If one of its arguments is tagged with the {@link Restriction} annotation, then it will
   *       be passed the current restriction being processed; the argument must be of type {@code
   *       RestrictionT}.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface NewTracker {}

  /**
   * Annotation for the method that maps an element and restriction to initial watermark estimator
   * state for a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>Signature: {@code WatermarkEstimatorStateT getInitialWatermarkState(<arguments>);}
   *
   * <p>This method must satisfy the following constraints:
   *
   * <ul>
   *   <li>The return type {@code WatermarkEstimatorStateT} defines the watermark state type used
   *       within this splittable DoFn. All other methods that use a {@link
   *       WatermarkEstimatorState @WatermarkEstimatorState} parameter must use the same type that
   *       is used here. It is suggested to use as narrow of a return type definition as possible
   *       (for example prefer to use a square type over a shape type as a square is a type of a
   *       shape).
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed; the argument must be of type {@code InputT}.
   *       Note that automatic conversion of {@link Row}s and {@link FieldAccess} parameters are
   *       currently unsupported.
   *   <li>If one of its arguments is tagged with the {@link Restriction} annotation, then it will
   *       be passed the current restriction being processed; the argument must be of type {@code
   *       RestrictionT}.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface GetInitialWatermarkEstimatorState {}

  /**
   * Annotation for the method that returns the coder to use for the watermark estimator state of a
   * <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>If not defined, a coder will be inferred using standard coder inference rules and the
   * pipeline's {@link Pipeline#getCoderRegistry coder registry}.
   *
   * <p>This method will be called only at pipeline construction time.
   *
   * <p>Signature: {@code Coder<WatermarkEstimatorStateT> getWatermarkEstimatorStateCoder();}
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface GetWatermarkEstimatorStateCoder {}

  /**
   * Annotation for the method that creates a new {@link WatermarkEstimator} for the watermark state
   * of a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
   *
   * <p>Signature: {@code MyWatermarkEstimator newWatermarkEstimator(<optional arguments>);}
   *
   * <p>If the return type is a subtype of {@link TimestampObservingWatermarkEstimator} then the
   * timestamp of each element output from this DoFn is provided to the watermark estimator.
   *
   * <p>This method must satisfy the following constraints:
   *
   * <ul>
   *   <li>The return type must be a subtype of {@code
   *       WatermarkEstimator<WatermarkEstimatorStateT>}. It is suggested to use as narrow of a
   *       return type definition as possible (for example prefer to use a square type over a shape
   *       type as a square is a type of a shape).
   *   <li>If one of its arguments is tagged with the {@link WatermarkEstimatorState} annotation,
   *       then it will be passed the current watermark estimator state; the argument must be of
   *       type {@code WatermarkEstimatorStateT}.
   *   <li>If one of its arguments is tagged with the {@link Element} annotation, then it will be
   *       passed the current element being processed; the argument must be of type {@code InputT}.
   *       Note that automatic conversion of {@link Row}s and {@link FieldAccess} parameters are
   *       currently unsupported.
   *   <li>If one of its arguments is tagged with the {@link Restriction} annotation, then it will
   *       be passed the current restriction being processed; the argument must be of type {@code
   *       RestrictionT}.
   *   <li>If one of its arguments is tagged with the {@link Timestamp} annotation, then it will be
   *       passed the timestamp of the current element being processed; the argument must be of type
   *       {@link Instant}.
   *   <li>If one of its arguments is a subtype of {@link BoundedWindow}, then it will be passed the
   *       window of the current element. When applied by {@link ParDo} the subtype of {@link
   *       BoundedWindow} must match the type of windows on the input {@link PCollection}. If the
   *       window is not accessed a runner may perform additional optimizations.
   *   <li>If one of its arguments is of type {@link PaneInfo}, then it will be passed information
   *       about the current triggering pane.
   *   <li>If one of the parameters is of type {@link PipelineOptions}, then it will be passed the
   *       options for the current pipeline.
   * </ul>
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface NewWatermarkEstimator {}

  /**
   * Parameter annotation for the watermark estimator state for the {@link NewWatermarkEstimator}
   * method. Must match the return type on the method annotated with {@link
   * GetInitialWatermarkEstimatorState}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface WatermarkEstimatorState {}

  /**
   * Annotation on a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}
   * specifying that the {@link DoFn} performs a bounded amount of work per input element, so
   * applying it to a bounded {@link PCollection} will produce also a bounded {@link PCollection}.
   * It is an error to specify this on a non-splittable {@link DoFn}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface BoundedPerElement {}

  /**
   * Annotation on a <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}
   * specifying that the {@link DoFn} performs an unbounded amount of work per input element, so
   * applying it to a bounded {@link PCollection} will produce an unbounded {@link PCollection}. It
   * is an error to specify this on a non-splittable {@link DoFn}.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public @interface UnboundedPerElement {}

  // This can't be put into ProcessContinuation itself due to the following problem:
  // http://ternarysearch.blogspot.com/2013/07/static-initialization-deadlock.html
  private static final ProcessContinuation PROCESS_CONTINUATION_STOP =
      new AutoValue_DoFn_ProcessContinuation(false, Duration.ZERO);

  /**
   * When used as a return value of {@link ProcessElement}, indicates whether there is more work to
   * be done for the current element.
   *
   * <p>If the {@link ProcessElement} call completes because of a failed {@code tryClaim()} call on
   * the {@link RestrictionTracker}, then the call MUST return {@link #stop()}.
   */
  @Experimental(Kind.SPLITTABLE_DO_FN)
  @AutoValue
  public abstract static class ProcessContinuation {
    /** Indicates that there is no more work to be done for the current element. */
    public static ProcessContinuation stop() {
      return PROCESS_CONTINUATION_STOP;
    }

    /** Indicates that there is more work to be done for the current element. */
    public static ProcessContinuation resume() {
      return new AutoValue_DoFn_ProcessContinuation(true, Duration.ZERO);
    }

    /**
     * If false, the {@link DoFn} promises that there is no more work remaining for the current
     * element, so the runner should not resume the {@link ProcessElement} call.
     */
    public abstract boolean shouldResume();

    /**
     * A minimum duration that should elapse between the end of this {@link ProcessElement} call and
     * the {@link ProcessElement} call continuing processing of the same element. By default, zero.
     */
    public abstract Duration resumeDelay();

    /** Builder method to set the value of {@link #resumeDelay()}. */
    public ProcessContinuation withResumeDelay(Duration resumeDelay) {
      return new AutoValue_DoFn_ProcessContinuation(shouldResume(), resumeDelay);
    }
  }

  /**
   * Finalize the {@link DoFn} construction to prepare for processing. This method should be called
   * by runners before any processing methods.
   *
   * @deprecated use {@link Setup} or {@link StartBundle} instead. This method will be removed in a
   *     future release.
   */
  @Deprecated
  public final void prepareForProcessing() {}

  /**
   * {@inheritDoc}
   *
   * <p>By default, does not register any display data. Implementors may override this method to
   * provide their own display data.
   */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {}

  /**
   * A parameter that is accessible during {@link StartBundle @StartBundle}, {@link
   * ProcessElement @ProcessElement} and {@link FinishBundle @FinishBundle} that allows the caller
   * to register a callback that will be invoked after the bundle has been successfully completed
   * and the runner has commit the output.
   *
   * <p>A common usage would be to perform any acknowledgements required by an external system such
   * as acking messages from a message queue since this callback is only invoked after the output of
   * the bundle has been durably persisted by the runner.
   *
   * <p>Note that a runner may make the output of the bundle available immediately to downstream
   * consumers without waiting for finalization to succeed. For pipelines that are sensitive to
   * duplicate messages, they must perform output deduplication in the pipeline.
   */
  @Experimental(Kind.PORTABILITY)
  public interface BundleFinalizer {
    /**
     * The provided function will be called after the runner successfully commits the output of a
     * successful bundle. Throwing during finalization represents that bundle finalization may have
     * failed and the runner may choose to attempt finalization again. The provided {@code
     * callbackExpiry} controls how long the finalization is valid for before it is garbage
     * collected and no longer able to be invoked.
     *
     * <p>Note that finalization is best effort and it is expected that the external system will
     * self recover state if finalization never happens or consistently fails. For example, a queue
     * based system that requires message acknowledgement would replay messages if that
     * acknowledgement was never received within the provided time bound.
     *
     * <p>See <a href="https://s.apache.org/beam-finalizing-bundles">Apache Beam Portability API:
     * How to Finalize Bundles</a> for further details.
     *
     * @param callbackExpiry When the finalization callback expires. If the runner cannot commit
     *     results and execute the callback within this duration, the callback will not be invoked.
     * @param callback The finalization callback method for the runner to invoke after processing
     *     results have been successfully committed.
     */
    void afterBundleCommit(Instant callbackExpiry, Callback callback);

    /**
     * An instance of a function that will be invoked after bundle finalization.
     *
     * <p>Note that this function should maintain all state necessary outside of a DoFn's context to
     * be able to perform bundle finalization and should not rely on mutable state stored within a
     * DoFn instance.
     */
    @FunctionalInterface
    interface Callback {
      void onBundleSuccess() throws Exception;
    }
  }
}
