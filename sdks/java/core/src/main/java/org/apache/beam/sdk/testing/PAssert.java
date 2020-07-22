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
package org.apache.beam.sdk.testing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An assertion on the contents of a {@link PCollection} incorporated into the pipeline. Such an
 * assertion can be checked no matter what kind of {@link PipelineRunner} is used.
 *
 * <p>Note that the {@code PAssert} call must precede the call to {@link Pipeline#run}.
 *
 * <p>Examples of use:
 *
 * <pre>{@code
 * Pipeline p = TestPipeline.create();
 * ...
 * PCollection<String> output =
 *      input
 *      .apply(ParDo.of(new TestDoFn()));
 * PAssert.that(output)
 *     .containsInAnyOrder("out1", "out2", "out3");
 * ...
 * PCollection<Integer> ints = ...
 * PCollection<Integer> sum =
 *     ints
 *     .apply(Combine.globally(new SumInts()));
 * PAssert.that(sum)
 *     .is(42);
 * ...
 * p.run();
 * }</pre>
 *
 * <p>JUnit and Hamcrest must be linked in by any code that uses PAssert.
 */
public class PAssert {
  public static final String SUCCESS_COUNTER = "PAssertSuccess";
  public static final String FAILURE_COUNTER = "PAssertFailure";
  private static final Counter successCounter =
      Metrics.counter(PAssert.class, PAssert.SUCCESS_COUNTER);
  private static final Counter failureCounter =
      Metrics.counter(PAssert.class, PAssert.FAILURE_COUNTER);

  private static int assertCount = 0;

  private static String nextAssertionName() {
    return "PAssert$" + (assertCount++);
  }

  // Do not instantiate.
  private PAssert() {}

  /**
   * A {@link DoFn} that counts the number of successful {@link SuccessOrFailure} in the input
   * {@link PCollection} and counts them. If a failed {@link SuccessOrFailure} is encountered, it is
   * counted and immediately raised.
   */
  private static final class DefaultConcludeFn extends DoFn<SuccessOrFailure, Void> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      SuccessOrFailure e = c.element();
      if (e.isSuccess()) {
        PAssert.successCounter.inc();
      } else {
        PAssert.failureCounter.inc();
        throw e.assertionError();
      }
    }
  }

  /**
   * Default transform to check that a PAssert was successful. This transform relies on two {@link
   * Counter} objects from the Metrics API to count the number of successful and failed asserts.
   * Runners that do not support the Metrics API should replace this transform with their own
   * implementation.
   */
  public static class DefaultConcludeTransform
      extends PTransform<PCollection<SuccessOrFailure>, PCollection<Void>> {
    @Override
    public PCollection<Void> expand(PCollection<SuccessOrFailure> input) {
      return input.apply(ParDo.of(new DefaultConcludeFn()));
    }
  }

  /**
   * Track the place where an assertion is defined. This is necessary because the stack trace of a
   * Throwable is a transient attribute, and can't be serialized. {@link PAssertionSite} helps track
   * the stack trace of the place where an assertion is issued.
   */
  public static class PAssertionSite implements Serializable {
    private final String message;
    private final StackTraceElement[] creationStackTrace;

    static PAssertionSite capture(String message) {
      return new PAssertionSite(message, new Throwable().getStackTrace());
    }

    PAssertionSite(String message, StackTraceElement[] creationStackTrace) {
      this.message = message;
      this.creationStackTrace = creationStackTrace;
    }

    public AssertionError wrap(Throwable t) {
      AssertionError res =
          new AssertionError(
              message.isEmpty() ? t.getMessage() : (message + ": " + t.getMessage()), t);
      res.setStackTrace(creationStackTrace);
      return res;
    }

    public AssertionError wrap(String message) {
      String outputMessage =
          (this.message == null || this.message.isEmpty())
              ? message
              : (this.message + ": " + message);
      AssertionError res = new AssertionError(outputMessage);
      res.setStackTrace(creationStackTrace);
      return res;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PAssertionSite that = (PAssertionSite) o;
      return Objects.equal(message, that.message)
          && Arrays.equals(creationStackTrace, that.creationStackTrace);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(message, Arrays.asList(creationStackTrace));
    }
  }

  /** Builder interface for assertions applicable to iterables and PCollection contents. */
  public interface IterableAssert<T> {
    /**
     * Creates a new {@link IterableAssert} like this one, but with the assertion restricted to only
     * run on the provided window.
     *
     * <p>The assertion will concatenate all panes present in the provided window if the {@link
     * Trigger} produces multiple panes. If the windowing strategy accumulates fired panes and
     * triggers fire multple times, consider using instead {@link #inFinalPane(BoundedWindow)} or
     * {@link #inOnTimePane(BoundedWindow)}.
     *
     * @return a new {@link IterableAssert} like this one but with the assertion only applied to the
     *     specified window.
     */
    IterableAssert<T> inWindow(BoundedWindow window);

    /**
     * Creates a new {@link IterableAssert} like this one, but with the assertion restricted to only
     * run on the provided window, running the checker only on the final pane for each key.
     *
     * <p>If the input {@link WindowingStrategy} does not always produce final panes, the assertion
     * may be executed over an empty input even if the trigger has fired previously. To ensure that
     * a final pane is always produced, set the {@link ClosingBehavior} of the windowing strategy
     * (via {@link Window#withAllowedLateness(Duration, ClosingBehavior)} setting {@link
     * ClosingBehavior} to {@link ClosingBehavior#FIRE_ALWAYS}).
     *
     * @return a new {@link IterableAssert} like this one but with the assertion only applied to the
     *     specified window.
     */
    IterableAssert<T> inFinalPane(BoundedWindow window);

    /**
     * Creates a new {@link IterableAssert} like this one, but with the assertion restricted to only
     * run on the provided window.
     *
     * @return a new {@link IterableAssert} like this one but with the assertion only applied to the
     *     specified window.
     */
    IterableAssert<T> inOnTimePane(BoundedWindow window);

    /**
     * Creates a new {@link IterableAssert} like this one, but with the assertion restricted to only
     * run on the provided window across all panes that were produced by the arrival of early data.
     *
     * @return a new {@link IterableAssert} like this one but with the assertion only applied to the
     *     specified window.
     */
    IterableAssert<T> inEarlyPane(BoundedWindow window);

    /**
     * Creates a new {@link IterableAssert} with the assertion restricted to only run on the
     * provided window across all panes that were produced by the arrival of late data.
     *
     * @return a new {@link IterableAssert} like this one but with the assertion only applied to the
     *     specified window.
     */
    IterableAssert<T> inLatePane(BoundedWindow window);

    /**
     * Creates a new {@link IterableAssert} like this one, but with the assertion restricted to only
     * run on the provided window across all panes that were not produced by the arrival of late
     * data.
     *
     * @return a new {@link IterableAssert} like this one but with the assertion only applied to the
     *     specified window.
     */
    IterableAssert<T> inCombinedNonLatePanes(BoundedWindow window);

    /**
     * Creates a new {@link IterableAssert} like this one, but with the assertion restricted to only
     * run on panes in the {@link GlobalWindow} that were emitted before the {@link GlobalWindow}
     * closed. These panes have {@link Timing#EARLY}.
     */
    IterableAssert<T> inEarlyGlobalWindowPanes();

    /**
     * Asserts that the iterable in question contains the provided elements.
     *
     * @return the same {@link IterableAssert} builder for further assertions
     */
    IterableAssert<T> containsInAnyOrder(T... expectedElements);

    /**
     * Asserts that the iterable in question contains the provided elements.
     *
     * @return the same {@link IterableAssert} builder for further assertions
     */
    IterableAssert<T> containsInAnyOrder(Iterable<T> expectedElements);

    /**
     * Asserts that the iterable in question is empty.
     *
     * @return the same {@link IterableAssert} builder for further assertions
     */
    IterableAssert<T> empty();

    /**
     * Applies the provided checking function (presumably containing assertions) to the iterable in
     * question.
     *
     * @return the same {@link IterableAssert} builder for further assertions
     */
    IterableAssert<T> satisfies(SerializableFunction<Iterable<T>, Void> checkerFn);
  }

  /** Builder interface for assertions applicable to a single value. */
  public interface SingletonAssert<T> {
    /**
     * Creates a new {@link SingletonAssert} like this one, but with the assertion restricted to
     * only run on the provided window.
     *
     * <p>The assertion will expect outputs to be produced to the provided window exactly once. If
     * the upstream {@link Trigger} may produce output multiple times, consider instead using {@link
     * #inFinalPane(BoundedWindow)} or {@link #inOnTimePane(BoundedWindow)}.
     *
     * @return a new {@link SingletonAssert} like this one but with the assertion only applied to
     *     the specified window.
     */
    SingletonAssert<T> inOnlyPane(BoundedWindow window);

    /**
     * Creates a new {@link SingletonAssert} like this one, but with the assertion restricted to
     * only run on the provided window, running the checker only on the final pane for each key.
     *
     * <p>If the input {@link WindowingStrategy} does not always produce final panes, the assertion
     * may be executed over an empty input even if the trigger has fired previously. To ensure that
     * a final pane is always produced, set the {@link ClosingBehavior} of the windowing strategy
     * (via {@link Window#withAllowedLateness(Duration, ClosingBehavior)} setting {@link
     * ClosingBehavior} to {@link ClosingBehavior#FIRE_ALWAYS}).
     *
     * @return a new {@link SingletonAssert} like this one but with the assertion only applied to
     *     the specified window.
     */
    SingletonAssert<T> inFinalPane(BoundedWindow window);

    /**
     * Creates a new {@link SingletonAssert} like this one, but with the assertion restricted to
     * only run on the provided window, running the checker only on the on-time pane for each key.
     *
     * @return a new {@link SingletonAssert} like this one but with the assertion only applied to
     *     the specified window.
     */
    SingletonAssert<T> inOnTimePane(BoundedWindow window);

    /**
     * Creates a new {@link SingletonAssert} like this one, but with the assertion restricted to
     * only run on the provided window, running the checker only on early panes for each key.
     *
     * @return a new {@link SingletonAssert} like this one but with the assertion only applied to
     *     the specified window.
     */
    SingletonAssert<T> inEarlyPane(BoundedWindow window);

    /**
     * Creates a new {@link SingletonAssert} with the assertion restricted to only run on the
     * provided window, running the checker only on late panes for each key.
     *
     * @return a new {@link SingletonAssert} like this one but with the assertion only applied to
     *     the specified window.
     */
    SingletonAssert<T> inLatePane(BoundedWindow window);

    /**
     * Asserts that the value in question is equal to the provided value, according to {@link
     * Object#equals}.
     *
     * @return the same {@link SingletonAssert} builder for further assertions
     */
    SingletonAssert<T> isEqualTo(T expected);

    /**
     * Asserts that the value in question is not equal to the provided value, according to {@link
     * Object#equals}.
     *
     * @return the same {@link SingletonAssert} builder for further assertions
     */
    SingletonAssert<T> notEqualTo(T notExpected);

    /**
     * Applies the provided checking function (presumably containing assertions) to the value in
     * question.
     *
     * @return the same {@link SingletonAssert} builder for further assertions
     */
    SingletonAssert<T> satisfies(SerializableFunction<T, Void> checkerFn);
  }

  /** Constructs an {@link IterableAssert} for the elements of the provided {@link PCollection}. */
  public static <T> IterableAssert<T> that(PCollection<T> actual) {
    return that(actual.getName(), actual);
  }

  /**
   * Constructs an {@link IterableAssert} for the elements of the provided {@link PCollection} with
   * the specified reason.
   */
  public static <T> IterableAssert<T> that(String reason, PCollection<T> actual) {
    return new PCollectionContentsAssert<>(actual, PAssertionSite.capture(reason));
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided {@link PCollection} which
   * must contain a single {@code Iterable<T>} value.
   */
  public static <T> IterableAssert<T> thatSingletonIterable(
      PCollection<? extends Iterable<T>> actual) {
    return thatSingletonIterable(actual.getName(), actual);
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided {@link PCollection } with
   * the specified reason. The provided PCollection must contain a single {@code Iterable<T>} value.
   */
  public static <T> IterableAssert<T> thatSingletonIterable(
      String reason, PCollection<? extends Iterable<T>> actual) {
    @SuppressWarnings("unchecked") // Safe covariant cast
    PCollection<Iterable<T>> actualIterables = (PCollection<Iterable<T>>) actual;

    return new PCollectionSingletonIterableAssert<>(
        actualIterables, PAssertionSite.capture(reason));
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@code PCollection
   * PCollection<T>}, which must be a singleton.
   */
  public static <T> SingletonAssert<T> thatSingleton(PCollection<T> actual) {
    return thatSingleton(actual.getName(), actual);
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@code PCollection
   * PCollection<T>} with the specified reason. The provided PCollection must be a singleton.
   */
  public static <T> SingletonAssert<T> thatSingleton(String reason, PCollection<T> actual) {
    return new PCollectionSingletonAssert<>(actual, PAssertionSite.capture(reason));
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection}.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any {@code Coder<K,
   * V>}.
   */
  public static <K, V> SingletonAssert<Map<K, Iterable<V>>> thatMultimap(
      PCollection<KV<K, V>> actual) {
    return thatMultimap(actual.getName(), actual);
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection} with the
   * specified reason.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any {@code Coder<K,
   * V>}.
   */
  public static <K, V> SingletonAssert<Map<K, Iterable<V>>> thatMultimap(
      String reason, PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();
    return new PCollectionViewAssert<>(
        actual,
        View.asMultimap(),
        MapCoder.of(kvCoder.getKeyCoder(), IterableCoder.of(kvCoder.getValueCoder())),
        PAssertionSite.capture(reason));
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection}, which
   * must have at most one value per key.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any {@code Coder<K,
   * V>}.
   */
  public static <K, V> SingletonAssert<Map<K, V>> thatMap(PCollection<KV<K, V>> actual) {
    return thatMap(actual.getName(), actual);
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection} with the
   * specified reason. The {@link PCollection} must have at most one value per key.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any {@code Coder<K,
   * V>}.
   */
  public static <K, V> SingletonAssert<Map<K, V>> thatMap(
      String reason, PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();
    return new PCollectionViewAssert<>(
        actual,
        View.asMap(),
        MapCoder.of(kvCoder.getKeyCoder(), kvCoder.getValueCoder()),
        PAssertionSite.capture(reason));
  }

  ////////////////////////////////////////////////////////////

  /**
   * An {@link IterableAssert} about the contents of a {@link PCollection}. This does not require
   * the runner to support side inputs.
   */
  protected static class PCollectionContentsAssert<T> implements IterableAssert<T> {
    private final PCollection<T> actual;
    private final AssertionWindows rewindowingStrategy;
    private final SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor;
    private final PAssertionSite site;

    public PCollectionContentsAssert(PCollection<T> actual, PAssertionSite site) {
      this(actual, IntoGlobalWindow.of(), PaneExtractors.allPanes(), site);
    }

    public PCollectionContentsAssert(
        PCollection<T> actual,
        AssertionWindows rewindowingStrategy,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor,
        PAssertionSite site) {
      this.actual = actual;
      this.rewindowingStrategy = rewindowingStrategy;
      this.paneExtractor = paneExtractor;
      this.site = site;
    }

    @Override
    public PCollectionContentsAssert<T> inWindow(BoundedWindow window) {
      return withPane(window, PaneExtractors.allPanes());
    }

    @Override
    public PCollectionContentsAssert<T> inFinalPane(BoundedWindow window) {
      return withPane(window, PaneExtractors.finalPane());
    }

    @Override
    public PCollectionContentsAssert<T> inOnTimePane(BoundedWindow window) {
      return withPane(window, PaneExtractors.onTimePane());
    }

    @Override
    public PCollectionContentsAssert<T> inEarlyPane(BoundedWindow window) {
      return withPane(window, PaneExtractors.earlyPanes());
    }

    @Override
    public PCollectionContentsAssert<T> inLatePane(BoundedWindow window) {
      return withPane(window, PaneExtractors.latePanes());
    }

    @Override
    public PCollectionContentsAssert<T> inCombinedNonLatePanes(BoundedWindow window) {
      return withPane(window, PaneExtractors.nonLatePanes());
    }

    @Override
    public IterableAssert<T> inEarlyGlobalWindowPanes() {
      return withPane(GlobalWindow.INSTANCE, PaneExtractors.earlyPanes());
    }

    private PCollectionContentsAssert<T> withPane(
        BoundedWindow window,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<BoundedWindow> windowCoder =
          (Coder) actual.getWindowingStrategy().getWindowFn().windowCoder();
      return new PCollectionContentsAssert<>(
          actual, IntoStaticWindows.of(windowCoder, window), paneExtractor, site);
    }

    /**
     * Checks that the {@code Iterable} contains the expected elements, in any order.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    @Override
    @SafeVarargs
    public final PCollectionContentsAssert<T> containsInAnyOrder(T... expectedElements) {
      return containsInAnyOrder(Arrays.asList(expectedElements));
    }

    /**
     * Checks that the {@code Iterable} contains the expected elements, in any order.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    @Override
    public PCollectionContentsAssert<T> containsInAnyOrder(Iterable<T> expectedElements) {
      return satisfies(new AssertContainsInAnyOrderRelation<>(), expectedElements);
    }

    @Override
    public PCollectionContentsAssert<T> empty() {
      containsInAnyOrder(Collections.emptyList());
      return this;
    }

    @Override
    public PCollectionContentsAssert<T> satisfies(
        SerializableFunction<Iterable<T>, Void> checkerFn) {
      actual.apply(
          nextAssertionName(),
          new GroupThenAssert<>(checkerFn, rewindowingStrategy, paneExtractor, site));
      return this;
    }

    /**
     * Checks that the {@code Iterable} contains elements that match the provided matchers, in any
     * order.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    @SafeVarargs
    final PCollectionContentsAssert<T> containsInAnyOrder(
        SerializableMatcher<? super T>... elementMatchers) {
      return satisfies(SerializableMatchers.containsInAnyOrder(elementMatchers));
    }

    /**
     * Applies a {@link SerializableFunction} to check the elements of the {@code Iterable}.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    private PCollectionContentsAssert<T> satisfies(
        AssertRelation<Iterable<T>, Iterable<T>> relation, Iterable<T> expectedElements) {
      return satisfies(
          new CheckRelationAgainstExpected<>(
              relation, expectedElements, IterableCoder.of(actual.getCoder())));
    }

    /**
     * Applies a {@link SerializableMatcher} to check the elements of the {@code Iterable}.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    PCollectionContentsAssert<T> satisfies(
        final SerializableMatcher<Iterable<? extends T>> matcher) {
      // Safe covariant cast. Could be elided by changing a lot of this file to use
      // more flexible bounds.
      @SuppressWarnings({"rawtypes", "unchecked"})
      SerializableFunction<Iterable<T>, Void> checkerFn =
          (SerializableFunction) new MatcherCheckerFn<>(matcher);
      actual.apply(
          "PAssert$" + (assertCount++),
          new GroupThenAssert<>(checkerFn, rewindowingStrategy, paneExtractor, site));
      return this;
    }

    /** Check that the passed-in matchers match the existing data. */
    protected static class MatcherCheckerFn<T> implements SerializableFunction<T, Void> {
      private SerializableMatcher<T> matcher;

      public MatcherCheckerFn(SerializableMatcher<T> matcher) {
        this.matcher = matcher;
      }

      @Override
      public @Nullable Void apply(T actual) {
        assertThat(actual, matcher);
        return null;
      }
    }

    /**
     * @throws UnsupportedOperationException always
     * @deprecated {@link Object#equals(Object)} is not supported on PAssert objects. If you meant
     *     to test object equality, use a variant of {@link #containsInAnyOrder} instead.
     */
    @Deprecated
    @Override
    public boolean equals(@Nullable Object o) {
      throw new UnsupportedOperationException(
          "If you meant to test object equality, use .containsInAnyOrder instead.");
    }

    /**
     * @throws UnsupportedOperationException always.
     * @deprecated {@link Object#hashCode()} is not supported on PAssert objects.
     */
    @Deprecated
    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(
          String.format("%s.hashCode() is not supported.", IterableAssert.class.getSimpleName()));
    }
  }

  /**
   * An {@link IterableAssert} for an iterable that is the sole element of a {@link PCollection}.
   * This does not require the runner to support side inputs.
   */
  private static class PCollectionSingletonIterableAssert<T> implements IterableAssert<T> {
    private final PCollection<Iterable<T>> actual;
    private final Coder<T> elementCoder;
    private final AssertionWindows rewindowingStrategy;
    private final SimpleFunction<Iterable<ValueInSingleWindow<Iterable<T>>>, Iterable<Iterable<T>>>
        paneExtractor;
    private final PAssertionSite site;

    public PCollectionSingletonIterableAssert(
        PCollection<Iterable<T>> actual, PAssertionSite site) {
      this(actual, IntoGlobalWindow.of(), PaneExtractors.allPanes(), site);
    }

    public PCollectionSingletonIterableAssert(
        PCollection<Iterable<T>> actual,
        AssertionWindows rewindowingStrategy,
        SimpleFunction<Iterable<ValueInSingleWindow<Iterable<T>>>, Iterable<Iterable<T>>>
            paneExtractor,
        PAssertionSite site) {
      this.actual = actual;

      @SuppressWarnings("unchecked")
      Coder<T> typedCoder = (Coder<T>) actual.getCoder().getCoderArguments().get(0);
      this.elementCoder = typedCoder;

      this.rewindowingStrategy = rewindowingStrategy;
      this.paneExtractor = paneExtractor;
      this.site = site;
    }

    @Override
    public PCollectionSingletonIterableAssert<T> inWindow(BoundedWindow window) {
      return withPanes(window, PaneExtractors.allPanes());
    }

    @Override
    public PCollectionSingletonIterableAssert<T> inFinalPane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.finalPane());
    }

    @Override
    public PCollectionSingletonIterableAssert<T> inOnTimePane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.onTimePane());
    }

    @Override
    public PCollectionSingletonIterableAssert<T> inEarlyPane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.earlyPanes());
    }

    @Override
    public PCollectionSingletonIterableAssert<T> inLatePane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.latePanes());
    }

    @Override
    public PCollectionSingletonIterableAssert<T> inCombinedNonLatePanes(BoundedWindow window) {
      return withPanes(window, PaneExtractors.nonLatePanes());
    }

    @Override
    public IterableAssert<T> inEarlyGlobalWindowPanes() {
      return withPanes(GlobalWindow.INSTANCE, PaneExtractors.earlyPanes());
    }

    private PCollectionSingletonIterableAssert<T> withPanes(
        BoundedWindow window,
        SimpleFunction<Iterable<ValueInSingleWindow<Iterable<T>>>, Iterable<Iterable<T>>>
            paneExtractor) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<BoundedWindow> windowCoder =
          (Coder) actual.getWindowingStrategy().getWindowFn().windowCoder();
      return new PCollectionSingletonIterableAssert<>(
          actual, IntoStaticWindows.of(windowCoder, window), paneExtractor, site);
    }

    @Override
    @SafeVarargs
    public final PCollectionSingletonIterableAssert<T> containsInAnyOrder(T... expectedElements) {
      return containsInAnyOrder(Arrays.asList(expectedElements));
    }

    @Override
    public PCollectionSingletonIterableAssert<T> empty() {
      return containsInAnyOrder(Collections.emptyList());
    }

    @Override
    public PCollectionSingletonIterableAssert<T> containsInAnyOrder(Iterable<T> expectedElements) {
      return satisfies(new AssertContainsInAnyOrderRelation<>(), expectedElements);
    }

    @Override
    public PCollectionSingletonIterableAssert<T> satisfies(
        SerializableFunction<Iterable<T>, Void> checkerFn) {
      actual.apply(
          "PAssert$" + (assertCount++),
          new GroupThenAssertForSingleton<>(checkerFn, rewindowingStrategy, paneExtractor, site));
      return this;
    }

    private PCollectionSingletonIterableAssert<T> satisfies(
        AssertRelation<Iterable<T>, Iterable<T>> relation, Iterable<T> expectedElements) {
      return satisfies(
          new CheckRelationAgainstExpected<>(
              relation, expectedElements, IterableCoder.of(elementCoder)));
    }
  }

  /**
   * A {@link SingletonAssert} about the contents of a {@link PCollection} when it contains a single
   * value of type {@code T}. This does not require the runner to support side inputs.
   */
  private static class PCollectionSingletonAssert<T> implements SingletonAssert<T> {
    private final PCollection<T> actual;
    private final Coder<T> coder;
    private final AssertionWindows rewindowingStrategy;
    private final SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor;

    private final PAssertionSite site;

    PCollectionSingletonAssert(PCollection<T> actual, PAssertionSite site) {
      this(actual, IntoGlobalWindow.of(), PaneExtractors.allPanes(), site);
    }

    PCollectionSingletonAssert(
        PCollection<T> actual,
        AssertionWindows rewindowingStrategy,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor,
        PAssertionSite site) {
      this.actual = actual;
      this.coder = actual.getCoder();
      this.rewindowingStrategy = rewindowingStrategy;
      this.paneExtractor = paneExtractor;
      this.site = site;
    }

    @Override
    public PCollectionSingletonAssert<T> inFinalPane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.finalPane());
    }

    @Override
    public PCollectionSingletonAssert<T> inOnTimePane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.onTimePane());
    }

    @Override
    public PCollectionSingletonAssert<T> inEarlyPane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.earlyPanes());
    }

    @Override
    public PCollectionSingletonAssert<T> inLatePane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.latePanes());
    }

    @Override
    public SingletonAssert<T> isEqualTo(T expected) {
      return satisfies(new AssertIsEqualToRelation<>(), expected);
    }

    @Override
    public SingletonAssert<T> notEqualTo(T notExpected) {
      return satisfies(new AssertNotEqualToRelation<>(), notExpected);
    }

    @Override
    public PCollectionSingletonAssert<T> inOnlyPane(BoundedWindow window) {
      return withPanes(window, PaneExtractors.onlyPane(site));
    }

    private PCollectionSingletonAssert<T> withPanes(
        BoundedWindow window,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<BoundedWindow> windowCoder =
          (Coder) actual.getWindowingStrategy().getWindowFn().windowCoder();
      return new PCollectionSingletonAssert<>(
          actual, IntoStaticWindows.of(windowCoder, window), paneExtractor, site);
    }

    @Override
    public PCollectionSingletonAssert<T> satisfies(SerializableFunction<T, Void> checkerFn) {
      actual.apply(
          "PAssert$" + (assertCount++),
          new GroupThenAssertForSingleton<>(checkerFn, rewindowingStrategy, paneExtractor, site));
      return this;
    }

    /**
     * Applies an {@link AssertRelation} to check the provided relation against the value of this
     * assert and the provided expected value.
     *
     * <p>Returns this {@code SingletonAssert}.
     */
    private PCollectionSingletonAssert<T> satisfies(
        AssertRelation<T, T> relation, final T expected) {
      return satisfies(new CheckRelationAgainstExpected<>(relation, expected, coder));
    }

    /**
     * @throws UnsupportedOperationException always
     * @deprecated {@link Object#equals(Object)} is not supported on PAssert objects. If you meant
     *     to test PCollection equality, use {@link #isEqualTo} instead.
     */
    @SuppressFBWarnings("EQ_UNUSUAL")
    @Deprecated
    @Override
    public boolean equals(@Nullable Object o) {
      throw new UnsupportedOperationException(
          String.format(
              "tests for Java equality of the %s object, not the PCollection in question. "
                  + "Call a test method, such as isEqualTo.",
              getClass().getSimpleName()));
    }

    /**
     * @throws UnsupportedOperationException always.
     * @deprecated {@link Object#hashCode()} is not supported on {@link PAssert} objects.
     */
    @Deprecated
    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(
          String.format("%s.hashCode() is not supported.", SingletonAssert.class.getSimpleName()));
    }
  }

  /**
   * An assertion about the contents of a {@link PCollection} when it is viewed as a single value of
   * type {@code ViewT}. This requires side input support from the runner.
   */
  private static class PCollectionViewAssert<ElemT, ViewT> implements SingletonAssert<ViewT> {
    private final PCollection<ElemT> actual;
    private final PTransform<PCollection<ElemT>, PCollectionView<ViewT>> view;
    private final AssertionWindows rewindowActuals;
    private final SimpleFunction<Iterable<ValueInSingleWindow<ElemT>>, Iterable<ElemT>>
        paneExtractor;
    private final Coder<ViewT> coder;
    private final PAssertionSite site;

    protected PCollectionViewAssert(
        PCollection<ElemT> actual,
        PTransform<PCollection<ElemT>, PCollectionView<ViewT>> view,
        Coder<ViewT> coder,
        PAssertionSite site) {
      this(actual, view, IntoGlobalWindow.of(), PaneExtractors.allPanes(), coder, site);
    }

    private PCollectionViewAssert(
        PCollection<ElemT> actual,
        PTransform<PCollection<ElemT>, PCollectionView<ViewT>> view,
        AssertionWindows rewindowActuals,
        SimpleFunction<Iterable<ValueInSingleWindow<ElemT>>, Iterable<ElemT>> paneExtractor,
        Coder<ViewT> coder,
        PAssertionSite site) {
      this.actual = actual;
      this.view = view;
      this.rewindowActuals = rewindowActuals;
      this.paneExtractor = paneExtractor;
      this.coder = coder;
      this.site = site;
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> inOnlyPane(BoundedWindow window) {
      return inPane(window, PaneExtractors.onlyPane(site));
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> inFinalPane(BoundedWindow window) {
      return inPane(window, PaneExtractors.finalPane());
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> inOnTimePane(BoundedWindow window) {
      return inPane(window, PaneExtractors.onTimePane());
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> inEarlyPane(BoundedWindow window) {
      return inPane(window, PaneExtractors.earlyPanes());
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> inLatePane(BoundedWindow window) {
      return inPane(window, PaneExtractors.latePanes());
    }

    private PCollectionViewAssert<ElemT, ViewT> inPane(
        BoundedWindow window,
        SimpleFunction<Iterable<ValueInSingleWindow<ElemT>>, Iterable<ElemT>> paneExtractor) {
      return new PCollectionViewAssert<>(
          actual,
          view,
          IntoStaticWindows.of(
              (Coder) actual.getWindowingStrategy().getWindowFn().windowCoder(), window),
          paneExtractor,
          coder,
          site);
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> isEqualTo(ViewT expectedValue) {
      return satisfies(new AssertIsEqualToRelation<>(), expectedValue);
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> notEqualTo(ViewT expectedValue) {
      return satisfies(new AssertNotEqualToRelation<>(), expectedValue);
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> satisfies(
        SerializableFunction<ViewT, Void> checkerFn) {
      actual
          .getPipeline()
          .apply(
              "PAssert$" + (assertCount++),
              new OneSideInputAssert<>(
                  CreateActual.from(actual, rewindowActuals, paneExtractor, view),
                  rewindowActuals.windowDummy(),
                  checkerFn,
                  site));
      return this;
    }

    /**
     * Applies an {@link AssertRelation} to check the provided relation against the value of this
     * assert and the provided expected value.
     *
     * <p>Returns this {@code SingletonAssert}.
     */
    private PCollectionViewAssert<ElemT, ViewT> satisfies(
        AssertRelation<ViewT, ViewT> relation, final ViewT expectedValue) {
      return satisfies(new CheckRelationAgainstExpected<>(relation, expectedValue, coder));
    }

    /**
     * @throws UnsupportedOperationException always
     * @deprecated {@link Object#equals(Object)} is not supported on PAssert objects. If you meant
     *     to test object equality, use {@link #isEqualTo} instead.
     */
    @Deprecated
    @Override
    public boolean equals(@Nullable Object o) {
      throw new UnsupportedOperationException(
          String.format(
              "tests for Java equality of the %s object, not the PCollection in question. "
                  + "Call a test method, such as isEqualTo.",
              getClass().getSimpleName()));
    }

    /**
     * @throws UnsupportedOperationException always.
     * @deprecated {@link Object#hashCode()} is not supported on {@link PAssert} objects.
     */
    @Deprecated
    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(
          String.format("%s.hashCode() is not supported.", SingletonAssert.class.getSimpleName()));
    }
  }

  ////////////////////////////////////////////////////////////////////////

  private static class CreateActual<T, ActualT>
      extends PTransform<PBegin, PCollectionView<ActualT>> {

    private final transient PCollection<T> actual;
    private final transient AssertionWindows rewindowActuals;
    private final transient SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>>
        extractPane;
    private final transient PTransform<PCollection<T>, PCollectionView<ActualT>> actualView;

    public static <T, ActualT> CreateActual<T, ActualT> from(
        PCollection<T> actual,
        AssertionWindows rewindowActuals,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> extractPane,
        PTransform<PCollection<T>, PCollectionView<ActualT>> actualView) {
      return new CreateActual<>(actual, rewindowActuals, extractPane, actualView);
    }

    private CreateActual(
        PCollection<T> actual,
        AssertionWindows rewindowActuals,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> extractPane,
        PTransform<PCollection<T>, PCollectionView<ActualT>> actualView) {
      this.actual = actual;
      this.rewindowActuals = rewindowActuals;
      this.extractPane = extractPane;
      this.actualView = actualView;
    }

    @Override
    public PCollectionView<ActualT> expand(PBegin input) {
      final Coder<T> coder = actual.getCoder();
      return actual
          .apply("FilterActuals", rewindowActuals.prepareActuals())
          .apply("GatherPanes", GatherAllPanes.globally())
          .apply("ExtractPane", MapElements.via(extractPane))
          .setCoder(IterableCoder.of(actual.getCoder()))
          .apply(Flatten.iterables())
          .apply("RewindowActuals", rewindowActuals.windowActuals())
          .apply(
              ParDo.of(
                  new DoFn<T, T>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) throws CoderException {
                      context.output(CoderUtils.clone(coder, context.element()));
                    }
                  }))
          .apply(actualView);
    }
  }

  /**
   * A partially applied {@link AssertRelation}, where one value is provided along with a coder to
   * serialize/deserialize them.
   */
  private static class CheckRelationAgainstExpected<T> implements SerializableFunction<T, Void> {
    private final AssertRelation<T, T> relation;
    private final byte[] encodedExpected;
    private final Coder<T> coder;

    public CheckRelationAgainstExpected(AssertRelation<T, T> relation, T expected, Coder<T> coder) {
      this.relation = relation;
      this.coder = coder;

      try {
        this.encodedExpected = CoderUtils.encodeToByteArray(coder, expected);
      } catch (IOException coderException) {
        throw new RuntimeException(coderException);
      }
    }

    @Override
    public Void apply(T actual) {
      try {
        T expected = CoderUtils.decodeFromByteArray(coder, encodedExpected);
        return relation.assertFor(expected).apply(actual);
      } catch (IOException coderException) {
        throw new RuntimeException(coderException);
      }
    }
  }

  /**
   * A transform that gathers the contents of a {@link PCollection} into a single main input
   * iterable in the global window. This requires a runner to support {@link GroupByKey} in the
   * global window, but not side inputs or other windowing or triggers unless the input is
   * non-trivially windowed or triggered.
   *
   * <p>If the {@link PCollection} is empty, this transform returns a {@link PCollection} containing
   * a single empty iterable, even though in practice most runners will not produce any element.
   */
  private static class GroupGlobally<T>
      extends PTransform<PCollection<T>, PCollection<Iterable<ValueInSingleWindow<T>>>>
      implements Serializable {
    private final AssertionWindows rewindowingStrategy;

    public GroupGlobally(AssertionWindows rewindowingStrategy) {
      this.rewindowingStrategy = rewindowingStrategy;
    }

    @Override
    public PCollection<Iterable<ValueInSingleWindow<T>>> expand(PCollection<T> input) {
      final int combinedKey = 42;

      if (input.getWindowingStrategy().equals(WindowingStrategy.globalDefault())
          && rewindowingStrategy instanceof IntoGlobalWindow) {
        // If we don't have to worry about complicated triggering semantics we can generate
        // a much simpler pipeline.  This is particularly useful for bootstrapping runners so that
        // we can run subsets of the validates runner test suite requiring support of only the
        // most basic primitives.

        // In order to ensure we actually get an (empty) iterable rather than an empty PCollection
        // when the input is an empty PCollection, we flatten with a dummy PCollection containing
        // an empty iterable before grouping on a singleton key and concatenating.
        PCollection<Iterable<ValueInSingleWindow<T>>> actual =
            input.apply(Reify.windows()).apply(ParDo.of(new ToSingletonIterables<>()));
        PCollection<Iterable<ValueInSingleWindow<T>>> dummy =
            input
                .getPipeline()
                .apply(
                    Create.<Iterable<ValueInSingleWindow<T>>>of(
                            ImmutableList.of(ImmutableList.of()))
                        .withCoder(actual.getCoder()));
        return PCollectionList.of(dummy)
            .and(actual)
            .apply(Flatten.pCollections())
            .apply(
                // Default end-of-window trigger disallowed for unbounded PCollections.
                input.isBounded() == PCollection.IsBounded.UNBOUNDED
                    ? Window.<Iterable<ValueInSingleWindow<T>>>configure()
                        .triggering(Never.ever())
                        .discardingFiredPanes()
                    : Window.<Iterable<ValueInSingleWindow<T>>>configure())
            .apply(WithKeys.of(combinedKey))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(ParDo.of(new ConcatFn<>()));
      }

      // Remove the triggering on both
      PTransform<
              PCollection<KV<Integer, Iterable<ValueInSingleWindow<T>>>>,
              PCollection<KV<Integer, Iterable<ValueInSingleWindow<T>>>>>
          removeTriggering =
              Window.<KV<Integer, Iterable<ValueInSingleWindow<T>>>>configure()
                  .triggering(Never.ever())
                  .discardingFiredPanes()
                  .withAllowedLateness(input.getWindowingStrategy().getAllowedLateness());
      // Group the contents by key. If it is empty, this PCollection will be empty, too.
      // Then key it again with a dummy key.
      PCollection<KV<Integer, Iterable<ValueInSingleWindow<T>>>> groupedContents =
          // TODO: Split the filtering from the rewindowing, and apply filtering before the Gather
          // if the grouping of extra records
          input
              .apply(rewindowingStrategy.prepareActuals())
              .apply("GatherAllOutputs", GatherAllPanes.globally())
              .apply("RewindowActuals", rewindowingStrategy.windowActuals())
              .apply("KeyForDummy", WithKeys.of(combinedKey))
              .apply("RemoveActualsTriggering", removeTriggering);

      // Create another non-empty PCollection that is keyed with a distinct dummy key
      PCollection<KV<Integer, Iterable<ValueInSingleWindow<T>>>> keyedDummy =
          input
              .getPipeline()
              .apply(
                  Create.of(
                          KV.of(
                              combinedKey,
                              (Iterable<ValueInSingleWindow<T>>)
                                  Collections.<ValueInSingleWindow<T>>emptyList()))
                      .withCoder(groupedContents.getCoder()))
              .apply("WindowIntoDummy", rewindowingStrategy.windowDummy())
              .apply("RemoveDummyTriggering", removeTriggering);

      // Flatten them together and group by the combined key to get a single element
      PCollection<KV<Integer, Iterable<Iterable<ValueInSingleWindow<T>>>>> dummyAndContents =
          PCollectionList.of(groupedContents)
              .and(keyedDummy)
              .apply("FlattenDummyAndContents", Flatten.pCollections())
              .apply(
                  "NeverTrigger",
                  Window.<KV<Integer, Iterable<ValueInSingleWindow<T>>>>configure()
                      .triggering(Never.ever())
                      .withAllowedLateness(input.getWindowingStrategy().getAllowedLateness())
                      .discardingFiredPanes())
              .apply("GroupDummyAndContents", GroupByKey.create());

      return dummyAndContents.apply(Values.create()).apply(ParDo.of(new ConcatFn<>()));
    }
  }

  private static final class ToSingletonIterables<T> extends DoFn<T, Iterable<T>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(ImmutableList.of(c.element()));
    }
  }

  private static final class ConcatFn<T> extends DoFn<Iterable<Iterable<T>>, Iterable<T>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(Iterables.concat(c.element()));
    }
  }

  /**
   * A transform that applies an assertion-checking function over iterables of {@code ActualT} to
   * the entirety of the contents of its input.
   */
  public static class GroupThenAssert<T> extends PTransform<PCollection<T>, PDone>
      implements Serializable {
    private final SerializableFunction<Iterable<T>, Void> checkerFn;
    private final AssertionWindows rewindowingStrategy;
    private final SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor;
    private final PAssertionSite site;

    private GroupThenAssert(
        SerializableFunction<Iterable<T>, Void> checkerFn,
        AssertionWindows rewindowingStrategy,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor,
        PAssertionSite site) {
      this.checkerFn = checkerFn;
      this.rewindowingStrategy = rewindowingStrategy;
      this.paneExtractor = paneExtractor;
      this.site = site;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input
          .apply("GroupGlobally", new GroupGlobally<>(rewindowingStrategy))
          .apply("GetPane", MapElements.via(paneExtractor))
          .setCoder(IterableCoder.of(input.getCoder()))
          .apply("RunChecks", ParDo.of(new GroupedValuesCheckerDoFn<>(checkerFn, site)))
          .apply("VerifyAssertions", new DefaultConcludeTransform());

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * A transform that applies an assertion-checking function to the sole element of a {@link
   * PCollection}.
   */
  public static class GroupThenAssertForSingleton<T> extends PTransform<PCollection<T>, PDone>
      implements Serializable {
    private final SerializableFunction<T, Void> checkerFn;
    private final AssertionWindows rewindowingStrategy;
    private final SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor;
    private final PAssertionSite site;

    private GroupThenAssertForSingleton(
        SerializableFunction<T, Void> checkerFn,
        AssertionWindows rewindowingStrategy,
        SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> paneExtractor,
        PAssertionSite site) {
      this.checkerFn = checkerFn;
      this.rewindowingStrategy = rewindowingStrategy;
      this.paneExtractor = paneExtractor;
      this.site = site;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input
          .apply("GroupGlobally", new GroupGlobally<>(rewindowingStrategy))
          .apply("GetPane", MapElements.via(paneExtractor))
          .setCoder(IterableCoder.of(input.getCoder()))
          .apply("RunChecks", ParDo.of(new SingletonCheckerDoFn<>(checkerFn, site)))
          .apply("VerifyAssertions", new DefaultConcludeTransform());

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * An assertion checker that takes a single {@link PCollectionView PCollectionView&lt;ActualT&gt;}
   * and an assertion over {@code ActualT}, and checks it within a Beam pipeline.
   *
   * <p>Note that the entire assertion must be serializable.
   *
   * <p>This is generally useful for assertion functions that are serializable but whose underlying
   * data may not have a coder.
   */
  public static class OneSideInputAssert<ActualT> extends PTransform<PBegin, PDone>
      implements Serializable {
    private final transient PTransform<PBegin, PCollectionView<ActualT>> createActual;
    private final transient PTransform<PCollection<Integer>, PCollection<Integer>> windowToken;
    private final SerializableFunction<ActualT, Void> checkerFn;
    private final PAssertionSite site;

    private OneSideInputAssert(
        PTransform<PBegin, PCollectionView<ActualT>> createActual,
        PTransform<PCollection<Integer>, PCollection<Integer>> windowToken,
        SerializableFunction<ActualT, Void> checkerFn,
        PAssertionSite site) {
      this.createActual = createActual;
      this.windowToken = windowToken;
      this.checkerFn = checkerFn;
      this.site = site;
    }

    @Override
    public PDone expand(PBegin input) {
      final PCollectionView<ActualT> actual = input.apply("CreateActual", createActual);

      input
          .apply(Create.of(0).withCoder(VarIntCoder.of()))
          .apply("WindowToken", windowToken)
          .apply(
              "RunChecks",
              ParDo.of(new SideInputCheckerDoFn<>(checkerFn, actual, site)).withSideInputs(actual))
          .apply("VerifyAssertions", new DefaultConcludeTransform());
      return PDone.in(input.getPipeline());
    }
  }

  /**
   * A {@link DoFn} that runs a checking {@link SerializableFunction} on the contents of a {@link
   * PCollectionView}, and adjusts counters and thrown exceptions for use in testing.
   *
   * <p>The input is ignored, but is {@link Integer} to be usable on runners that do not support
   * null values.
   */
  private static class SideInputCheckerDoFn<ActualT> extends DoFn<Integer, SuccessOrFailure> {
    private final SerializableFunction<ActualT, Void> checkerFn;
    private final PCollectionView<ActualT> actual;
    private final PAssertionSite site;

    private SideInputCheckerDoFn(
        SerializableFunction<ActualT, Void> checkerFn,
        PCollectionView<ActualT> actual,
        PAssertionSite site) {
      this.checkerFn = checkerFn;
      this.actual = actual;
      this.site = site;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      ActualT actualContents = c.sideInput(actual);
      c.output(doChecks(site, actualContents, checkerFn));
    }
  }

  /**
   * A {@link DoFn} that runs a checking {@link SerializableFunction} on the contents of the single
   * iterable element of the input {@link PCollection} and adjusts counters and thrown exceptions
   * for use in testing.
   *
   * <p>The singleton property is presumed, not enforced.
   */
  private static class GroupedValuesCheckerDoFn<ActualT> extends DoFn<ActualT, SuccessOrFailure> {
    private final SerializableFunction<ActualT, Void> checkerFn;
    private final PAssertionSite site;

    private GroupedValuesCheckerDoFn(
        SerializableFunction<ActualT, Void> checkerFn, PAssertionSite site) {
      this.checkerFn = checkerFn;
      this.site = site;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(doChecks(site, c.element(), checkerFn));
    }
  }

  /**
   * A {@link DoFn} that runs a checking {@link SerializableFunction} on the contents of the single
   * item contained within the single iterable on input and adjusts counters and thrown exceptions
   * for use in testing.
   *
   * <p>The singleton property of the input {@link PCollection} is presumed, not enforced. However,
   * each input element must be a singleton iterable, or this will fail.
   */
  private static class SingletonCheckerDoFn<ActualT>
      extends DoFn<Iterable<ActualT>, SuccessOrFailure> {
    private final SerializableFunction<ActualT, Void> checkerFn;
    private final PAssertionSite site;

    private SingletonCheckerDoFn(
        SerializableFunction<ActualT, Void> checkerFn, PAssertionSite site) {
      this.checkerFn = checkerFn;
      this.site = site;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      ActualT actualContents = Iterables.getOnlyElement(c.element());
      c.output(doChecks(site, actualContents, checkerFn));
    }
  }

  protected static <ActualT> SuccessOrFailure doChecks(
      PAssertionSite site, ActualT actualContents, SerializableFunction<ActualT, Void> checkerFn) {
    try {
      checkerFn.apply(actualContents);
      return SuccessOrFailure.success();
    } catch (Throwable t) {
      return SuccessOrFailure.failure(site, t);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link SerializableFunction} that verifies that an actual value is equal to an expected
   * value.
   */
  private static class AssertIsEqualTo<T> implements SerializableFunction<T, Void> {
    private T expected;

    public AssertIsEqualTo(T expected) {
      this.expected = expected;
    }

    @Override
    public @Nullable Void apply(T actual) {
      assertThat(actual, equalTo(expected));
      return null;
    }
  }

  /**
   * A {@link SerializableFunction} that verifies that an actual value is not equal to an expected
   * value.
   */
  private static class AssertNotEqualTo<T> implements SerializableFunction<T, Void> {
    private T expected;

    public AssertNotEqualTo(T expected) {
      this.expected = expected;
    }

    @Override
    public @Nullable Void apply(T actual) {
      assertThat(actual, not(equalTo(expected)));
      return null;
    }
  }

  /**
   * A {@link SerializableFunction} that verifies that an {@code Iterable} contains expected items
   * in any order.
   */
  private static class AssertContainsInAnyOrder<T>
      implements SerializableFunction<Iterable<T>, Void> {
    private T[] expected;

    @SafeVarargs
    public AssertContainsInAnyOrder(T... expected) {
      this.expected = expected;
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInAnyOrder(Collection<T> expected) {
      this((T[]) expected.toArray());
    }

    public AssertContainsInAnyOrder(Iterable<T> expected) {
      this(Lists.newArrayList(expected));
    }

    @Override
    public @Nullable Void apply(Iterable<T> actual) {
      assertThat(actual, containsInAnyOrder(expected));
      return null;
    }
  }

  ////////////////////////////////////////////////////////////

  /**
   * A binary predicate between types {@code Actual} and {@code Expected}. Implemented as a method
   * {@code assertFor(Expected)} which returns a {@code SerializableFunction<Actual, Void>} that
   * should verify the assertion..
   */
  private interface AssertRelation<ActualT, ExpectedT> extends Serializable {
    SerializableFunction<ActualT, Void> assertFor(ExpectedT input);
  }

  /** An {@link AssertRelation} implementing the binary predicate that two objects are equal. */
  private static class AssertIsEqualToRelation<T> implements AssertRelation<T, T> {
    @Override
    public SerializableFunction<T, Void> assertFor(T expected) {
      return new AssertIsEqualTo<>(expected);
    }
  }

  /** An {@link AssertRelation} implementing the binary predicate that two objects are not equal. */
  private static class AssertNotEqualToRelation<T> implements AssertRelation<T, T> {
    @Override
    public SerializableFunction<T, Void> assertFor(T expected) {
      return new AssertNotEqualTo<>(expected);
    }
  }

  /**
   * An {@code AssertRelation} implementing the binary predicate that two collections are equal
   * modulo reordering.
   */
  private static class AssertContainsInAnyOrderRelation<T>
      implements AssertRelation<Iterable<T>, Iterable<T>> {
    @Override
    public SerializableFunction<Iterable<T>, Void> assertFor(Iterable<T> expectedElements) {
      return new AssertContainsInAnyOrder<>(expectedElements);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * A strategy for filtering and rewindowing the actual and dummy {@link PCollection PCollections}
   * within a {@link PAssert}.
   *
   * <p>This must ensure that the windowing strategies of the output of {@link #windowActuals()} and
   * {@link #windowDummy()} are compatible (and can be {@link Flatten Flattened}).
   *
   * <p>The {@link PCollection} produced by {@link #prepareActuals()} will be a parent (though not a
   * direct parent) of the transform provided to {@link #windowActuals()}.
   */
  private interface AssertionWindows {
    /**
     * Returns a transform that assigns the dummy element into the appropriate {@link BoundedWindow
     * windows}.
     */
    <T> PTransform<PCollection<T>, PCollection<T>> windowDummy();

    /**
     * Returns a transform that filters and reassigns windows of the actual elements if necessary.
     */
    <T> PTransform<PCollection<T>, PCollection<T>> prepareActuals();

    /**
     * Returns a transform that assigns the actual elements into the appropriate {@link
     * BoundedWindow windows}. Will be called after {@link #prepareActuals()}.
     */
    <T> PTransform<PCollection<T>, PCollection<T>> windowActuals();
  }

  /** An {@link AssertionWindows} which assigns all elements to the {@link GlobalWindow}. */
  private static class IntoGlobalWindow implements AssertionWindows, Serializable {
    public static AssertionWindows of() {
      return new IntoGlobalWindow();
    }

    private <T> PTransform<PCollection<T>, PCollection<T>> window() {
      return Window.into(new GlobalWindows());
    }

    @Override
    public <T> PTransform<PCollection<T>, PCollection<T>> windowDummy() {
      return window();
    }

    /**
     * Rewindows all input elements into the {@link GlobalWindow}. This ensures that the result
     * PCollection will contain all of the elements of the PCollection when the window is not
     * specified.
     */
    @Override
    public <T> PTransform<PCollection<T>, PCollection<T>> prepareActuals() {
      return window();
    }

    @Override
    public <T> PTransform<PCollection<T>, PCollection<T>> windowActuals() {
      return window();
    }
  }

  private static class IntoStaticWindows implements AssertionWindows {
    private final StaticWindows windowFn;

    public static AssertionWindows of(Coder<BoundedWindow> windowCoder, BoundedWindow window) {
      return new IntoStaticWindows(StaticWindows.of(windowCoder, window));
    }

    private IntoStaticWindows(StaticWindows windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    public <T> PTransform<PCollection<T>, PCollection<T>> windowDummy() {
      return Window.into(windowFn);
    }

    @Override
    public <T> PTransform<PCollection<T>, PCollection<T>> prepareActuals() {
      return new FilterWindows<>(windowFn);
    }

    @Override
    public <T> PTransform<PCollection<T>, PCollection<T>> windowActuals() {
      return Window.into(windowFn.intoOnlyExisting());
    }
  }

  /** A DoFn that filters elements based on their presence in a static collection of windows. */
  private static final class FilterWindows<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final StaticWindows windows;

    public FilterWindows(StaticWindows windows) {
      this.windows = windows;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input.apply("FilterWindows", ParDo.of(new Fn()));
    }

    private class Fn extends DoFn<T, T> {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
        if (windows.getWindows().contains(window)) {
          c.output(c.element());
        }
      }
    }
  }

  public static int countAsserts(Pipeline pipeline) {
    AssertionCountingVisitor visitor = new AssertionCountingVisitor();
    pipeline.traverseTopologically(visitor);
    return visitor.getPAssertCount();
  }

  /**
   * A {@link PipelineVisitor} that counts the number of total {@link PAssert PAsserts} in a {@link
   * Pipeline}.
   */
  private static class AssertionCountingVisitor extends PipelineVisitor.Defaults {
    private int assertCount;
    private boolean pipelineVisited;

    private AssertionCountingVisitor() {
      assertCount = 0;
      pipelineVisited = false;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {
      if (node.isRootNode()) {
        checkState(
            !pipelineVisited,
            "Tried to visit a pipeline with an already used %s",
            AssertionCountingVisitor.class.getSimpleName());
      }
      if (!node.isRootNode()
          && (node.getTransform() instanceof PAssert.OneSideInputAssert
              || node.getTransform() instanceof PAssert.GroupThenAssert
              || node.getTransform() instanceof PAssert.GroupThenAssertForSingleton)) {
        assertCount++;
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(Node node) {
      if (node.isRootNode()) {
        pipelineVisited = true;
      }
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      if (node.getTransform() instanceof PAssert.OneSideInputAssert
          || node.getTransform() instanceof PAssert.GroupThenAssert
          || node.getTransform() instanceof PAssert.GroupThenAssertForSingleton) {
        assertCount++;
      }
    }

    /** Gets the number of {@link PAssert PAsserts} in the pipeline. */
    int getPAssertCount() {
      checkState(pipelineVisited);
      return assertCount;
    }
  }
}
