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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An assertion on the contents of a {@link PCollection} incorporated into the pipeline. Such an
 * assertion can be checked no matter what kind of {@link PipelineRunner} is used.
 *
 * <p>Note that the {@code PAssert} call must precede the call to {@link Pipeline#run}.
 *
 * <p>Examples of use: <pre>{@code
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

  private static final Logger LOG = LoggerFactory.getLogger(PAssert.class);

  public static final String SUCCESS_COUNTER = "PAssertSuccess";
  public static final String FAILURE_COUNTER = "PAssertFailure";

  private static int assertCount = 0;

  // Do not instantiate.
  private PAssert() {}

  /**
   * Builder interface for assertions applicable to iterables and PCollection contents.
   */
  public interface IterableAssert<T> {

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
     * Applies the provided checking function (presumably containing assertions) to the
     * iterable in question.
     *
     * @return the same {@link IterableAssert} builder for further assertions
     */
    IterableAssert<T> satisfies(SerializableFunction<Iterable<T>, Void> checkerFn);
  }

  /**
   * Builder interface for assertions applicable to a single value.
   */
  public interface SingletonAssert<T> {
    /**
     * Asserts that the value in question is equal to the provided value, according to
     * {@link Object#equals}.
     *
     * @return the same {@link SingletonAssert} builder for further assertions
     */
    SingletonAssert<T> isEqualTo(T expected);

    /**
     * Asserts that the value in question is not equal to the provided value, according
     * to {@link Object#equals}.
     *
     * @return the same {@link SingletonAssert} builder for further assertions
     */
    SingletonAssert<T> notEqualTo(T notExpected);

    /**
     * Applies the provided checking function (presumably containing assertions) to the
     * value in question.
     *
     * @return the same {@link SingletonAssert} builder for further assertions
     */
    SingletonAssert<T> satisfies(SerializableFunction<T, Void> checkerFn);
  }

  /**
   * Constructs an {@link IterableAssert} for the elements of the provided {@link PCollection}.
   */
  public static <T> IterableAssert<T> that(PCollection<T> actual) {
    return new PCollectionContentsAssert<>(actual);
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided {@link PCollection} which
   * must contain a single {@code Iterable<T>} value.
   */
  public static <T> IterableAssert<T> thatSingletonIterable(
      PCollection<? extends Iterable<T>> actual) {

    try {
    } catch (NoSuchElementException | IllegalArgumentException exc) {
      throw new IllegalArgumentException(
          "PAssert.<T>thatSingletonIterable requires a PCollection<Iterable<T>>"
              + " with a Coder<Iterable<T>> where getCoderArguments() yields a"
              + " single Coder<T> to apply to the elements.");
    }

    @SuppressWarnings("unchecked") // Safe covariant cast
    PCollection<Iterable<T>> actualIterables = (PCollection<Iterable<T>>) actual;

    return new PCollectionSingletonIterableAssert<>(actualIterables);
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided
   * {@code PCollection PCollection<T>}, which must be a singleton.
   */
  public static <T> SingletonAssert<T> thatSingleton(PCollection<T> actual) {
    return new PCollectionViewAssert<>(actual, View.<T>asSingleton(), actual.getCoder());
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection}.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any
   * {@code Coder<K, V>}.
   */
  public static <K, V> SingletonAssert<Map<K, Iterable<V>>> thatMultimap(
      PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();
    return new PCollectionViewAssert<>(
        actual,
        View.<K, V>asMultimap(),
        MapCoder.of(kvCoder.getKeyCoder(), IterableCoder.of(kvCoder.getValueCoder())));
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided {@link PCollection}, which
   * must have at most one value per key.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any
   * {@code Coder<K, V>}.
   */
  public static <K, V> SingletonAssert<Map<K, V>> thatMap(PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();
    return new PCollectionViewAssert<>(
        actual, View.<K, V>asMap(), MapCoder.of(kvCoder.getKeyCoder(), kvCoder.getValueCoder()));
  }

  ////////////////////////////////////////////////////////////

  /**
   * An {@link IterableAssert} about the contents of a {@link PCollection}. This does not require
   * the runner to support side inputs.
   */
  private static class PCollectionContentsAssert<T> implements IterableAssert<T> {
    private final PCollection<T> actual;

    public PCollectionContentsAssert(PCollection<T> actual) {
      this.actual = actual;
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
      return satisfies(new AssertContainsInAnyOrderRelation<T>(), expectedElements);
    }

    @Override
    public PCollectionContentsAssert<T> empty() {
      return containsInAnyOrder(Collections.<T>emptyList());
    }

    @Override
    public PCollectionContentsAssert<T> satisfies(
        SerializableFunction<Iterable<T>, Void> checkerFn) {
      actual.apply("PAssert$" + (assertCount++), new GroupThenAssert<>(checkerFn));
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
      return satisfies(SerializableMatchers.<T>containsInAnyOrder(elementMatchers));
    }

    /**
     * Applies a {@link SerializableFunction} to check the elements of the {@code Iterable}.
     *
     * <p>Returns this {@code IterableAssert}.
     */
    private PCollectionContentsAssert<T> satisfies(
        AssertRelation<Iterable<T>, Iterable<T>> relation, Iterable<T> expectedElements) {
      return satisfies(
          new CheckRelationAgainstExpected<Iterable<T>>(
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
      actual.apply("PAssert$" + (assertCount++), new GroupThenAssert<>(checkerFn));
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
     * @throws UnsupportedOperationException always
     * @deprecated {@link Object#equals(Object)} is not supported on PAssert objects. If you meant
     * to test object equality, use a variant of {@link #containsInAnyOrder} instead.
     */
    @Deprecated
    @Override
    public boolean equals(Object o) {
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

    public PCollectionSingletonIterableAssert(PCollection<Iterable<T>> actual) {
      this.actual = actual;

      @SuppressWarnings("unchecked")
      Coder<T> typedCoder = (Coder<T>) actual.getCoder().getCoderArguments().get(0);
      this.elementCoder = typedCoder;
    }

    @Override
    @SafeVarargs
    public final PCollectionSingletonIterableAssert<T> containsInAnyOrder(T... expectedElements) {
      return containsInAnyOrder(Arrays.asList(expectedElements));
    }

    @Override
    public PCollectionSingletonIterableAssert<T> empty() {
      return containsInAnyOrder(Collections.<T>emptyList());
    }

    @Override
    public PCollectionSingletonIterableAssert<T> containsInAnyOrder(Iterable<T> expectedElements) {
      return satisfies(new AssertContainsInAnyOrderRelation<T>(), expectedElements);
    }

    @Override
    public PCollectionSingletonIterableAssert<T> satisfies(
        SerializableFunction<Iterable<T>, Void> checkerFn) {
      actual.apply("PAssert$" + (assertCount++), new GroupThenAssertForSingleton<>(checkerFn));
      return this;
    }

    private PCollectionSingletonIterableAssert<T> satisfies(
        AssertRelation<Iterable<T>, Iterable<T>> relation, Iterable<T> expectedElements) {
      return satisfies(
          new CheckRelationAgainstExpected<Iterable<T>>(
              relation, expectedElements, IterableCoder.of(elementCoder)));
    }
  }

  /**
   * An assertion about the contents of a {@link PCollection} when it is viewed as a single value
   * of type {@code ViewT}. This requires side input support from the runner.
   */
  private static class PCollectionViewAssert<ElemT, ViewT> implements SingletonAssert<ViewT> {
    private final PCollection<ElemT> actual;
    private final PTransform<PCollection<ElemT>, PCollectionView<ViewT>> view;
    private final Coder<ViewT> coder;

    protected PCollectionViewAssert(
        PCollection<ElemT> actual,
        PTransform<PCollection<ElemT>, PCollectionView<ViewT>> view,
        Coder<ViewT> coder) {
      this.actual = actual;
      this.view = view;
      this.coder = coder;
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> isEqualTo(ViewT expectedValue) {
      return satisfies(new AssertIsEqualToRelation<ViewT>(), expectedValue);
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> notEqualTo(ViewT expectedValue) {
      return satisfies(new AssertNotEqualToRelation<ViewT>(), expectedValue);
    }

    @Override
    public PCollectionViewAssert<ElemT, ViewT> satisfies(
        SerializableFunction<ViewT, Void> checkerFn) {
      actual
          .getPipeline()
          .apply(
              "PAssert$" + (assertCount++),
              new OneSideInputAssert<ViewT>(CreateActual.from(actual, view), checkerFn));
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
      return satisfies(new CheckRelationAgainstExpected<ViewT>(relation, expectedValue, coder));
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
    private final transient PTransform<PCollection<T>, PCollectionView<ActualT>> actualView;

    public static <T, ActualT> CreateActual<T, ActualT> from(
        PCollection<T> actual, PTransform<PCollection<T>, PCollectionView<ActualT>> actualView) {
      return new CreateActual<>(actual, actualView);
    }

    private CreateActual(
        PCollection<T> actual, PTransform<PCollection<T>, PCollectionView<ActualT>> actualView) {
      this.actual = actual;
      this.actualView = actualView;
    }

    @Override
    public PCollectionView<ActualT> apply(PBegin input) {
      final Coder<T> coder = actual.getCoder();
      return actual
          .apply(Window.<T>into(new GlobalWindows()))
          .apply(
              ParDo.of(
                  new DoFn<T, T>() {
                    @Override
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
   * global window, but not side inputs or other windowing or triggers.
   */
  private static class GroupGlobally<T> extends PTransform<PCollection<T>, PCollection<Iterable<T>>>
      implements Serializable {

    public GroupGlobally() {}

    @Override
    public PCollection<Iterable<T>> apply(PCollection<T> input) {
      return input
          .apply("GloballyWindow", Window.<T>into(new GlobalWindows()))
          .apply("DummyKey", WithKeys.<Integer, T>of(0))
          .apply("GroupByKey", GroupByKey.<Integer, T>create())
          .apply("GetOnlyValue", Values.<Iterable<T>>create());
    }
  }

  /**
   * A transform that applies an assertion-checking function over iterables of {@code ActualT} to
   * the entirety of the contents of its input.
   */
  public static class GroupThenAssert<T> extends PTransform<PCollection<T>, PDone>
      implements Serializable {
    private final SerializableFunction<Iterable<T>, Void> checkerFn;

    private GroupThenAssert(SerializableFunction<Iterable<T>, Void> checkerFn) {
      this.checkerFn = checkerFn;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      input
          .apply("GroupGlobally", new GroupGlobally<T>())
          .apply(
              "RunChecks",
              ParDo.of(
                  new DoFn<Iterable<T>, Void>() {
                    @Override
                    public void processElement(ProcessContext context) {
                      checkerFn.apply(context.element());
                    }
                  }));

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * A transform that applies an assertion-checking function to a single iterable contained as the
   * sole element of a {@link PCollection}.
   */
  public static class GroupThenAssertForSingleton<T>
      extends PTransform<PCollection<Iterable<T>>, PDone> implements Serializable {
    private final SerializableFunction<Iterable<T>, Void> checkerFn;

    private GroupThenAssertForSingleton(SerializableFunction<Iterable<T>, Void> checkerFn) {
      this.checkerFn = checkerFn;
    }

    @Override
    public PDone apply(PCollection<Iterable<T>> input) {
      input
          .apply("GroupGlobally", new GroupGlobally<Iterable<T>>())
          .apply(
              "RunChecks",
              ParDo.of(
                  new DoFn<Iterable<Iterable<T>>, Void>() {
                    @Override
                    public void processElement(ProcessContext context) {
                      checkerFn.apply(Iterables.getOnlyElement(context.element()));
                    }
                  }));

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * An assertion checker that takes a single {@link PCollectionView
   * PCollectionView&lt;ActualT&gt;} and an assertion over {@code ActualT}, and checks it within a
   * Beam pipeline.
   *
   * <p>Note that the entire assertion must be serializable.
   *
   * <p>This is generally useful for assertion functions that are serializable but whose underlying
   * data may not have a coder.
   */
  public static class OneSideInputAssert<ActualT> extends PTransform<PBegin, PDone>
      implements Serializable {
    private final transient PTransform<PBegin, PCollectionView<ActualT>> createActual;
    private final SerializableFunction<ActualT, Void> checkerFn;

    private OneSideInputAssert(
        PTransform<PBegin, PCollectionView<ActualT>> createActual,
        SerializableFunction<ActualT, Void> checkerFn) {
      this.createActual = createActual;
      this.checkerFn = checkerFn;
    }

    @Override
    public PDone apply(PBegin input) {
      final PCollectionView<ActualT> actual = input.apply("CreateActual", createActual);

      input
          .apply(Create.of(0).withCoder(VarIntCoder.of()))
          .apply(
              ParDo.named("RunChecks")
                  .withSideInputs(actual)
                  .of(new CheckerDoFn<>(checkerFn, actual)));

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * A {@link DoFn} that runs a checking {@link SerializableFunction} on the contents of a
   * {@link PCollectionView}, and adjusts counters and thrown exceptions for use in testing.
   *
   * <p>The input is ignored, but is {@link Integer} to be usable on runners that do not support
   * null values.
   */
  private static class CheckerDoFn<ActualT> extends DoFn<Integer, Void> {
    private final SerializableFunction<ActualT, Void> checkerFn;
    private final Aggregator<Integer, Integer> success =
        createAggregator(SUCCESS_COUNTER, new Sum.SumIntegerFn());
    private final Aggregator<Integer, Integer> failure =
        createAggregator(FAILURE_COUNTER, new Sum.SumIntegerFn());
    private final PCollectionView<ActualT> actual;

    private CheckerDoFn(
        SerializableFunction<ActualT, Void> checkerFn, PCollectionView<ActualT> actual) {
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
        LOG.error("PAssert failed expectations.", t);
        failure.addValue(1);
        // TODO: allow for metrics to propagate on failure when running a streaming pipeline
        if (!c.getPipelineOptions().as(StreamingOptions.class).isStreaming()) {
          throw t;
        }
      }
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
    public Void apply(T actual) {
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
    public Void apply(T actual) {
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
   * A binary predicate between types {@code Actual} and {@code Expected}. Implemented as a method
   * {@code assertFor(Expected)} which returns a {@code SerializableFunction<Actual, Void>} that
   * should verify the assertion..
   */
  private static interface AssertRelation<ActualT, ExpectedT> extends Serializable {
    public SerializableFunction<ActualT, Void> assertFor(ExpectedT input);
  }

  /**
   * An {@link AssertRelation} implementing the binary predicate that two objects are equal.
   */
  private static class AssertIsEqualToRelation<T> implements AssertRelation<T, T> {
    @Override
    public SerializableFunction<T, Void> assertFor(T expected) {
      return new AssertIsEqualTo<T>(expected);
    }
  }

  /**
   * An {@link AssertRelation} implementing the binary predicate that two objects are not equal.
   */
  private static class AssertNotEqualToRelation<T> implements AssertRelation<T, T> {
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
