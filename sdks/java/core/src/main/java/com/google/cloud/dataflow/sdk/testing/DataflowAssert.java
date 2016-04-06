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
package com.google.cloud.dataflow.sdk.testing;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An assertion on the contents of a {@link PCollection}
 * incorporated into the pipeline.  Such an assertion
 * can be checked no matter what kind of {@link PipelineRunner} is
 * used.
 *
 * <p>Note that the {@code DataflowAssert} call must precede the call
 * to {@link Pipeline#run}.
 *
 * <p>Examples of use:
 * <pre>{@code
 * Pipeline p = TestPipeline.create();
 * ...
 * PCollection<String> output =
 *      input
 *      .apply(ParDo.of(new TestDoFn()));
 * DataflowAssert.that(output)
 *     .containsInAnyOrder("out1", "out2", "out3");
 * ...
 * PCollection<Integer> ints = ...
 * PCollection<Integer> sum =
 *     ints
 *     .apply(Combine.globally(new SumInts()));
 * DataflowAssert.that(sum)
 *     .is(42);
 * ...
 * p.run();
 * }</pre>
 *
 * <p>JUnit and Hamcrest must be linked in by any code that uses DataflowAssert.
 */
public class DataflowAssert {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowAssert.class);

  static final String SUCCESS_COUNTER = "DataflowAssertSuccess";
  static final String FAILURE_COUNTER = "DataflowAssertFailure";

  private static int assertCount = 0;

  // Do not instantiate.
  private DataflowAssert() {}

  /**
   * Constructs an {@link IterableAssert} for the elements of the provided
   * {@link PCollection}.
   */
  public static <T> IterableAssert<T> that(PCollection<T> actual) {
    return new IterableAssert<>(
        new CreateActual<T, Iterable<T>>(actual, View.<T>asIterable()),
         actual.getPipeline())
         .setCoder(actual.getCoder());
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided
   * {@link PCollection} which must contain a single {@code Iterable<T>}
   * value.
   */
  public static <T> IterableAssert<T>
      thatSingletonIterable(PCollection<? extends Iterable<T>> actual) {

    List<? extends Coder<?>> maybeElementCoder = actual.getCoder().getCoderArguments();
    Coder<T> tCoder;
    try {
      @SuppressWarnings("unchecked")
      Coder<T> tCoderTmp = (Coder<T>) Iterables.getOnlyElement(maybeElementCoder);
      tCoder = tCoderTmp;
    } catch (NoSuchElementException | IllegalArgumentException exc) {
      throw new IllegalArgumentException(
        "DataflowAssert.<T>thatSingletonIterable requires a PCollection<Iterable<T>>"
        + " with a Coder<Iterable<T>> where getCoderArguments() yields a"
        + " single Coder<T> to apply to the elements.");
    }

    @SuppressWarnings("unchecked") // Safe covariant cast
    PCollection<Iterable<T>> actualIterables = (PCollection<Iterable<T>>) actual;

    return new IterableAssert<>(
        new CreateActual<Iterable<T>, Iterable<T>>(
            actualIterables, View.<Iterable<T>>asSingleton()),
        actual.getPipeline())
        .setCoder(tCoder);
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided
   * {@code PCollectionView PCollectionView<Iterable<T>>}.
   */
  public static <T> IterableAssert<T> thatIterable(PCollectionView<Iterable<T>> actual) {
    return new IterableAssert<>(new PreExisting<Iterable<T>>(actual), actual.getPipeline());
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided
   * {@code PCollection PCollection<T>}, which must be a singleton.
   */
  public static <T> SingletonAssert<T> thatSingleton(PCollection<T> actual) {
    return new SingletonAssert<>(
        new CreateActual<T, T>(actual, View.<T>asSingleton()), actual.getPipeline())
        .setCoder(actual.getCoder());
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection}.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder},
   * not just any {@code Coder<K, V>}.
   */
  public static <K, V> SingletonAssert<Map<K, Iterable<V>>>
      thatMultimap(PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();

    return new SingletonAssert<>(
        new CreateActual<>(actual, View.<K, V>asMultimap()), actual.getPipeline())
        .setCoder(MapCoder.of(kvCoder.getKeyCoder(), IterableCoder.of(kvCoder.getValueCoder())));
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection},
   * which must have at most one value per key.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder},
   * not just any {@code Coder<K, V>}.
   */
  public static <K, V> SingletonAssert<Map<K, V>> thatMap(PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();

    return new SingletonAssert<>(
        new CreateActual<>(actual, View.<K, V>asMap()), actual.getPipeline())
        .setCoder(MapCoder.of(kvCoder.getKeyCoder(), kvCoder.getValueCoder()));
  }

  ////////////////////////////////////////////////////////////

  /**
   * An assertion about the contents of a {@link PCollectionView} yielding an {@code Iterable<T>}.
   */
  public static class IterableAssert<T> implements Serializable {
    private final Pipeline pipeline;
    private final PTransform<PBegin, PCollectionView<Iterable<T>>> createActual;
    private Optional<Coder<T>> coder;

    protected IterableAssert(
        PTransform<PBegin, PCollectionView<Iterable<T>>> createActual, Pipeline pipeline) {
      this.createActual = createActual;
      this.pipeline = pipeline;
      this.coder = Optional.absent();
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed for internal purposes.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> setCoder(Coder<T> coderOrNull) {
      this.coder = Optional.fromNullable(coderOrNull);
      return this;
    }

    /**
     * Gets the coder, which may yet be absent.
     */
    public Coder<T> getCoder() {
      if (coder.isPresent()) {
        return coder.get();
      } else {
        throw new IllegalStateException(
            "Attempting to access the coder of an IterableAssert"
                + " that has not been set yet.");
      }
    }

    /**
     * Applies a {@link SerializableFunction} to check the elements of the {@code Iterable}.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> satisfies(SerializableFunction<Iterable<T>, Void> checkerFn) {
      pipeline.apply(
          "DataflowAssert$" + (assertCount++),
          new OneSideInputAssert<Iterable<T>>(createActual, checkerFn));
      return this;
    }

    /**
     * Applies a {@link SerializableFunction} to check the elements of the {@code Iterable}.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> satisfies(
        AssertRelation<Iterable<T>, Iterable<T>> relation,
        final Iterable<T> expectedElements) {
      pipeline.apply(
          "DataflowAssert$" + (assertCount++),
          new TwoSideInputAssert<Iterable<T>, Iterable<T>>(createActual,
              new CreateExpected<T, Iterable<T>>(expectedElements, coder, View.<T>asIterable()),
              relation));

      return this;
    }

    /**
     * Applies a {@link SerializableMatcher} to check the elements of the {@code Iterable}.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    IterableAssert<T> satisfies(final SerializableMatcher<Iterable<? extends T>> matcher) {
      // Safe covariant cast. Could be elided by changing a lot of this file to use
      // more flexible bounds.
      @SuppressWarnings({"rawtypes", "unchecked"})
      SerializableFunction<Iterable<T>, Void> checkerFn =
        (SerializableFunction) new MatcherCheckerFn<>(matcher);
      pipeline.apply(
          "DataflowAssert$" + (assertCount++),
          new OneSideInputAssert<Iterable<T>>(
              createActual,
              checkerFn));
      return this;
    }

    private static class MatcherCheckerFn<T> implements SerializableFunction<T, Void> {
      private SerializableMatcher<T> matcher;

      public MatcherCheckerFn(SerializableMatcher<T> matcher) {
        this.matcher = matcher;
      }

      @Override
      public Void apply(T actual) {
        assertThat(actual, matcher);
        return null;
      }
    }

    /**
     * Checks that the {@code Iterable} is empty.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> empty() {
      return satisfies(new AssertContainsInAnyOrderRelation<T>(), Collections.<T>emptyList());
    }

    /**
     * @throws UnsupportedOperationException always
     * @deprecated {@link Object#equals(Object)} is not supported on DataflowAssert objects.
     *    If you meant to test object equality, use a variant of {@link #containsInAnyOrder}
     *    instead.
     */
    @Deprecated
    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException(
          "If you meant to test object equality, use .containsInAnyOrder instead.");
    }

    /**
     * @throws UnsupportedOperationException always.
     * @deprecated {@link Object#hashCode()} is not supported on DataflowAssert objects.
     */
    @Deprecated
    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(
          String.format("%s.hashCode() is not supported.", IterableAssert.class.getSimpleName()));
    }

    /**
     * Checks that the {@code Iterable} contains the expected elements, in any
     * order.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> containsInAnyOrder(Iterable<T> expectedElements) {
      return satisfies(new AssertContainsInAnyOrderRelation<T>(), expectedElements);
    }

    /**
     * Checks that the {@code Iterable} contains the expected elements, in any
     * order.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    @SafeVarargs
    public final IterableAssert<T> containsInAnyOrder(T... expectedElements) {
      return satisfies(
        new AssertContainsInAnyOrderRelation<T>(),
        Arrays.asList(expectedElements));
    }

    /**
     * Checks that the {@code Iterable} contains elements that match the provided matchers,
     * in any order.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    @SafeVarargs
    final IterableAssert<T> containsInAnyOrder(
        SerializableMatcher<? super T>... elementMatchers) {
      return satisfies(SerializableMatchers.<T>containsInAnyOrder(elementMatchers));
    }
  }

  /**
   * An assertion about the single value of type {@code T}
   * associated with a {@link PCollectionView}.
   */
  public static class SingletonAssert<T> implements Serializable {
    private final Pipeline pipeline;
    private final CreateActual<?, T> createActual;
    private Optional<Coder<T>> coder;

    protected SingletonAssert(
        CreateActual<?, T> createActual, Pipeline pipeline) {
      this.pipeline = pipeline;
      this.createActual = createActual;
      this.coder = Optional.absent();
    }

    /**
     * Always throws an {@link UnsupportedOperationException}: users are probably looking for
     * {@link #isEqualTo}.
     */
    @Deprecated
    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException(
          String.format(
              "tests for Java equality of the %s object, not the PCollection in question. "
                  + "Call a test method, such as isEqualTo.",
              getClass().getSimpleName()));
    }

    /**
     * @throws UnsupportedOperationException always.
     * @deprecated {@link Object#hashCode()} is not supported on DataflowAssert objects.
     */
    @Deprecated
    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(
          String.format("%s.hashCode() is not supported.", SingletonAssert.class.getSimpleName()));
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed
     * for internal purposes.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    public SingletonAssert<T> setCoder(Coder<T> coderOrNull) {
      this.coder = Optional.fromNullable(coderOrNull);
      return this;
    }

    /**
     * Gets the coder, which may yet be absent.
     */
    public Coder<T> getCoder() {
      if (coder.isPresent()) {
        return coder.get();
      } else {
        throw new IllegalStateException(
            "Attempting to access the coder of an IterableAssert that has not been set yet.");
      }
    }

    /**
     * Applies a {@link SerializableFunction} to check the value of this
     * {@code SingletonAssert}'s view.
     *
     * <p>Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> satisfies(SerializableFunction<T, Void> checkerFn) {
      pipeline.apply(
          "DataflowAssert$" + (assertCount++),
          new OneSideInputAssert<T>(createActual, checkerFn));
      return this;
    }

    /**
     * Applies an {@link AssertRelation} to check the provided relation against the
     * value of this assert and the provided expected value.
     *
     * <p>Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> satisfies(
        AssertRelation<T, T> relation,
        final T expectedValue) {
      pipeline.apply(
          "DataflowAssert$" + (assertCount++),
          new TwoSideInputAssert<T, T>(createActual,
              new CreateExpected<T, T>(Arrays.asList(expectedValue), coder, View.<T>asSingleton()),
              relation));

      return this;
    }

    /**
     * Checks that the value of this {@code SingletonAssert}'s view is equal
     * to the expected value.
     *
     * <p>Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> isEqualTo(T expectedValue) {
      return satisfies(new AssertIsEqualToRelation<T>(), expectedValue);
    }

    /**
     * Checks that the value of this {@code SingletonAssert}'s view is not equal
     * to the expected value.
     *
     * <p>Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> notEqualTo(T expectedValue) {
      return satisfies(new AssertNotEqualToRelation<T>(), expectedValue);
    }

    /**
     * Checks that the value of this {@code SingletonAssert}'s view is equal to
     * the expected value.
     *
     * @deprecated replaced by {@link #isEqualTo}
     */
    @Deprecated
    public SingletonAssert<T> is(T expectedValue) {
      return isEqualTo(expectedValue);
    }

  }

  ////////////////////////////////////////////////////////////////////////

  private static class CreateActual<T, ActualT>
      extends PTransform<PBegin, PCollectionView<ActualT>> {

    private final transient PCollection<T> actual;
    private final transient PTransform<PCollection<T>, PCollectionView<ActualT>> actualView;

    private CreateActual(PCollection<T> actual,
        PTransform<PCollection<T>, PCollectionView<ActualT>> actualView) {
      this.actual = actual;
      this.actualView = actualView;
    }

    @Override
    public PCollectionView<ActualT> apply(PBegin input) {
      final Coder<T> coder = actual.getCoder();
      return actual
          .apply(Window.<T>into(new GlobalWindows()))
          .apply(ParDo.of(new DoFn<T, T>() {
            @Override
            public void processElement(ProcessContext context) throws CoderException {
              context.output(CoderUtils.clone(coder, context.element()));
            }
          }))
          .apply(actualView);
    }
  }

  private static class CreateExpected<T, ExpectedT>
      extends PTransform<PBegin, PCollectionView<ExpectedT>> {

    private final Iterable<T> elements;
    private final Optional<Coder<T>> coder;
    private final transient PTransform<PCollection<T>, PCollectionView<ExpectedT>> view;

    private CreateExpected(Iterable<T> elements, Optional<Coder<T>> coder,
        PTransform<PCollection<T>, PCollectionView<ExpectedT>> view) {
      this.elements = elements;
      this.coder = coder;
      this.view = view;
    }

    @Override
    public PCollectionView<ExpectedT> apply(PBegin input) {
      Create.Values<T> createTransform = Create.<T>of(elements);
      if (coder.isPresent()) {
        createTransform = createTransform.withCoder(coder.get());
      }
      return input.apply(createTransform).apply(view);
    }
  }

  private static class PreExisting<T> extends PTransform<PBegin, PCollectionView<T>> {

    private final PCollectionView<T> view;

    private PreExisting(PCollectionView<T> view) {
      this.view = view;
    }

    @Override
    public PCollectionView<T> apply(PBegin input) {
      return view;
    }
  }

  /**
   * An assertion checker that takes a single
   * {@link PCollectionView PCollectionView&lt;ActualT&gt;}
   * and an assertion over {@code ActualT}, and checks it within a dataflow
   * pipeline.
   *
   * <p>Note that the entire assertion must be serializable. If
   * you need to make assertions involving multiple inputs
   * that are each not serializable, use TwoSideInputAssert.
   *
   * <p>This is generally useful for assertion functions that
   * are serializable but whose underlying data may not have a coder.
   */
  static class OneSideInputAssert<ActualT>
      extends PTransform<PBegin, PDone> implements Serializable {
    private final transient PTransform<PBegin, PCollectionView<ActualT>> createActual;
    private final SerializableFunction<ActualT, Void> checkerFn;

    public OneSideInputAssert(
        PTransform<PBegin, PCollectionView<ActualT>> createActual,
        SerializableFunction<ActualT, Void> checkerFn) {
      this.createActual = createActual;
      this.checkerFn = checkerFn;
    }

    @Override
    public PDone apply(PBegin input) {
      final PCollectionView<ActualT> actual = input.apply("CreateActual", createActual);

      input
          .apply(Create.<Void>of((Void) null).withCoder(VoidCoder.of()))
          .apply(ParDo.named("RunChecks").withSideInputs(actual)
              .of(new CheckerDoFn<>(checkerFn, actual)));

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * A {@link DoFn} that runs a checking {@link SerializableFunction} on the contents of
   * a {@link PCollectionView}, and adjusts counters and thrown exceptions for use in testing.
   */
  private static class CheckerDoFn<ActualT> extends DoFn<Void, Void> {
    private final SerializableFunction<ActualT, Void> checkerFn;
    private final Aggregator<Integer, Integer> success =
        createAggregator(SUCCESS_COUNTER, new Sum.SumIntegerFn());
    private final Aggregator<Integer, Integer> failure =
        createAggregator(FAILURE_COUNTER, new Sum.SumIntegerFn());
    private final PCollectionView<ActualT> actual;

    private CheckerDoFn(
        SerializableFunction<ActualT, Void> checkerFn,
        PCollectionView<ActualT> actual) {
      this.checkerFn = checkerFn;
      this.actual = actual;
    }

    @Override
    public void processElement(ProcessContext c) {
      try {
        ActualT actualContents = c.sideInput(actual);
        checkerFn.apply(actualContents);
        success.addValue(1);
      } catch (Throwable t) {
        LOG.error("DataflowAssert failed expectations.", t);
        failure.addValue(1);
        // TODO: allow for metrics to propagate on failure when running a streaming pipeline
        if (!c.getPipelineOptions().as(StreamingOptions.class).isStreaming()) {
          throw t;
        }
      }
    }
  }

  /**
   * An assertion checker that takes a {@link PCollectionView PCollectionView&lt;ActualT&gt;},
   * a {@link PCollectionView PCollectionView&lt;ExpectedT&gt;}, a relation
   * over {@code A} and {@code B}, and checks that the relation holds
   * within a dataflow pipeline.
   *
   * <p>This is useful when either/both of {@code A} and {@code B}
   * are not serializable, but have coders (provided
   * by the underlying {@link PCollection}s).
   */
  static class TwoSideInputAssert<ActualT, ExpectedT>
      extends PTransform<PBegin, PDone> implements Serializable {

    private final transient PTransform<PBegin, PCollectionView<ActualT>> createActual;
    private final transient PTransform<PBegin, PCollectionView<ExpectedT>> createExpected;
    private final AssertRelation<ActualT, ExpectedT> relation;

    protected TwoSideInputAssert(
        PTransform<PBegin, PCollectionView<ActualT>> createActual,
        PTransform<PBegin, PCollectionView<ExpectedT>> createExpected,
        AssertRelation<ActualT, ExpectedT> relation) {
      this.createActual = createActual;
      this.createExpected = createExpected;
      this.relation = relation;
    }

    @Override
    public PDone apply(PBegin input) {
      final PCollectionView<ActualT> actual = input.apply("CreateActual", createActual);
      final PCollectionView<ExpectedT> expected = input.apply("CreateExpected", createExpected);

      input
          .apply(Create.<Void>of((Void) null).withCoder(VoidCoder.of()))
          .apply(ParDo.named("RunChecks").withSideInputs(actual, expected)
              .of(new CheckerDoFn<>(relation, actual, expected)));

      return PDone.in(input.getPipeline());
    }

    private static class CheckerDoFn<ActualT, ExpectedT> extends DoFn<Void, Void> {
      private final Aggregator<Integer, Integer> success =
          createAggregator(SUCCESS_COUNTER, new Sum.SumIntegerFn());
      private final Aggregator<Integer, Integer> failure =
          createAggregator(FAILURE_COUNTER, new Sum.SumIntegerFn());
      private final AssertRelation<ActualT, ExpectedT> relation;
      private final PCollectionView<ActualT> actual;
      private final PCollectionView<ExpectedT> expected;

      private CheckerDoFn(AssertRelation<ActualT, ExpectedT> relation,
          PCollectionView<ActualT> actual, PCollectionView<ExpectedT> expected) {
        this.relation = relation;
        this.actual = actual;
        this.expected = expected;
      }

      @Override
      public void processElement(ProcessContext c) {
        try {
          ActualT actualContents = c.sideInput(actual);
          ExpectedT expectedContents = c.sideInput(expected);
          relation.assertFor(expectedContents).apply(actualContents);
          success.addValue(1);
        } catch (Throwable t) {
          LOG.error("DataflowAssert failed expectations.", t);
          failure.addValue(1);
          // TODO: allow for metrics to propagate on failure when running a streaming pipeline
          if (!c.getPipelineOptions().as(StreamingOptions.class).isStreaming()) {
            throw t;
          }
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link SerializableFunction} that verifies that an actual value is equal to an
   * expected value.
   */
  private static class AssertIsEqualTo<T> implements SerializableFunction<T, Void> {
    private T expected;

    public AssertIsEqualTo(T expected) {
      this.expected = expected;
    }

    @Override
    public Void apply(T actual) {
      assertThat(actual, equalTo(expected));
      return null;
    }
  }

  /**
   * A {@link SerializableFunction} that verifies that an actual value is not equal to an
   * expected value.
   */
  private static class AssertNotEqualTo<T> implements SerializableFunction<T, Void> {
    private T expected;

    public AssertNotEqualTo(T expected) {
      this.expected = expected;
    }

    @Override
    public Void apply(T actual) {
      assertThat(actual, not(equalTo(expected)));
      return null;
    }
  }

  /**
   * A {@link SerializableFunction} that verifies that an {@code Iterable} contains
   * expected items in any order.
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
      this(Lists.<T>newArrayList(expected));
    }

    @Override
    public Void apply(Iterable<T> actual) {
      assertThat(actual, containsInAnyOrder(expected));
      return null;
    }
  }

  ////////////////////////////////////////////////////////////

  /**
   * A binary predicate between types {@code Actual} and {@code Expected}.
   * Implemented as a method {@code assertFor(Expected)} which returns
   * a {@code SerializableFunction<Actual, Void>}
   * that should verify the assertion..
   */
  private static interface AssertRelation<ActualT, ExpectedT> extends Serializable {
    public SerializableFunction<ActualT, Void> assertFor(ExpectedT input);
  }

  /**
   * An {@link AssertRelation} implementing the binary predicate that two objects are equal.
   */
  private static class AssertIsEqualToRelation<T>
      implements AssertRelation<T, T> {
    @Override
    public SerializableFunction<T, Void> assertFor(T expected) {
      return new AssertIsEqualTo<T>(expected);
    }
  }

  /**
   * An {@link AssertRelation} implementing the binary predicate that two objects are not equal.
   */
  private static class AssertNotEqualToRelation<T>
      implements AssertRelation<T, T> {
    @Override
    public SerializableFunction<T, Void> assertFor(T expected) {
      return new AssertNotEqualTo<T>(expected);
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
      return new AssertContainsInAnyOrder<T>(expectedElements);
    }
  }
}
