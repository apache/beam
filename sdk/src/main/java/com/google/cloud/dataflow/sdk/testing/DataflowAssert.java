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

package com.google.cloud.dataflow.sdk.testing;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
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
 * <p> Examples of use:
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

  // Do not instantiate.
  private DataflowAssert() {}

  /**
   * Constructs an {@link IterableAssert} for the elements of the provided
   * {@link PCollection PCollection&lt;T&gt;}.
   */
  public static <T> IterableAssert<T> that(PCollection<T> actual) {
    return
        new IterableAssert<>(inGlobalWindows(actual).apply(View.<T>asIterable()))
        .setCoder(actual.getCoder());
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided
   * {@link PCollection PCollection&lt;Iterable&lt;T&gt;&gt;}, which must be a
   * singleton.
   */
  public static <T> IterableAssert<T>
      thatSingletonIterable(PCollection<? extends Iterable<T>> actual) {

    List<? extends Coder<?>> maybeElementCoder = actual.getCoder().getCoderArguments();
    Coder<T> tCoder;
    try {
      tCoder = (Coder<T>) Iterables.getOnlyElement(maybeElementCoder);
    } catch (NoSuchElementException | IllegalArgumentException exc) {
      throw new IllegalArgumentException(
        "DataflowAssert.<T>thatSingletonIterable requires a PCollection<Iterable<T>>"
        + " with a Coder<Iterable<T>> where getCoderArguments() yields a"
        + " single Coder<T> to apply to the elements.");
    }

    @SuppressWarnings("unchecked") // Safe covariant cast
    PCollection<Iterable<T>> actualIterables = (PCollection<Iterable<T>>) actual;

    return new IterableAssert<T>(
            inGlobalWindows(actualIterables)
            .apply(View.<Iterable<T>>asSingleton()))
        .setCoder(tCoder);
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided
   * {@code PCollectionView PCollectionView<Iterable<T>>}.
   */
  public static <T> IterableAssert<T> thatIterable(PCollectionView<Iterable<T>> actual) {
    return new IterableAssert<>(actual);
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided
   * {@code PCollection PCollection<T>}, which must be a singleton.
   */
  public static <T> SingletonAssert<T> thatSingleton(PCollection<T> actual) {
    return new SingletonAssert<>(inGlobalWindows(actual).apply(View.<T>asSingleton()))
        .setCoder(actual.getCoder());
  }

  /**
   * Constructs a {@link SingletonAssert SingletonAssert<Map<K, Iterable<V>>>}
   * for the value of the provided {@link PCollection PCollection<KV<K, V>>}
   *
   * <p> Note that the actual value must be coded by a {@link KvCoder},
   * not just any {@code Coder<K, V>}.
   */
  public static <K, V> SingletonAssert<Map<K, Iterable<V>>>
      thatMultimap(PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();
    return new SingletonAssert<>(inGlobalWindows(actual).apply(View.<K, V>asMap()))
        .setCoder(MapCoder.of(kvCoder.getKeyCoder(), IterableCoder.of(kvCoder.getValueCoder())));
  }

  /**
   * Constructs a {@link SingletonAssert SingletonAssert<Map<K, V>>} for the value of the provided
   * {@link PCollection PCollection<KV<K, V>>}, which must have at
   * most one value per key.

   * <p> Note that the actual value must be coded by a {@link KvCoder},
   * not just any {@code Coder<K, V>}.
   */
  public static <K, V> SingletonAssert<Map<K, V>> thatMap(PCollection<KV<K, V>> actual) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) actual.getCoder();
    return new SingletonAssert<>(
        inGlobalWindows(actual).apply(View.<K, V>asMap().withSingletonValues()))
        .setCoder(MapCoder.of(kvCoder.getKeyCoder(), kvCoder.getValueCoder()));
  }

  ////////////////////////////////////////////////////////////

  /**
   * An assertion about the contents of a
   * {@link PCollectionView PCollectionView&lt;Iterable&lt;T&gt;&gt;}.
   */
  @SuppressWarnings("serial")
  public static class IterableAssert<T> implements Serializable {

    private final PCollectionView<Iterable<T>> actualView;
    private Optional<Coder<T>> coder;

    protected IterableAssert(PCollectionView<Iterable<T>> actualView) {
      this.actualView = actualView;
      coder = Optional.absent();
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed
     * for internal purposes.
     *
     * <p> Returns this {@code IterableAssert}.
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
     * <p> Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> satisfies(SerializableFunction<Iterable<T>, Void> checkerFn) {
      new OneSideInputAssert<Iterable<T>>(actualView).satisfies(checkerFn);
      return this;
    }

    /**
     * Applies a {@link SerializableFunction} to check the elements of the {@code Iterable}.
     *
     * <p> Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> satisfies(
        AssertRelation<Iterable<T>, Iterable<T>> relation,
        Iterable<T> expectedElements) {
      new TwoSideInputAssert<Iterable<T>, Iterable<T>>(actualView,
          actualView.getPipeline()
              .apply(Create.of(expectedElements))
              .setCoder(getCoder())
              .apply(View.<T>asIterable()))
          .satisfies(relation);
      return this;
    }

    /**
     * Checks that the {@code Iterable} contains the expected elements, in any
     * order.
     *
     * <p> Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> containsInAnyOrder(Iterable<T> expectedElements) {
      return satisfies(new AssertContainsInAnyOrderRelation<T>(), expectedElements);
    }

    /**
     * Checks that the {@code Iterable} contains the expected elements, in any
     * order.
     *
     * <p> Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> containsInAnyOrder(T... expectedElements) {
      return satisfies(
        new AssertContainsInAnyOrderRelation<T>(),
        Arrays.asList(expectedElements));
    };
  }

  /**
   * An assertion about the single value of type {@code T}
   * associated with a {@link PCollectionView PCollectionView&lt;T&gt;}.
   */
  @SuppressWarnings("serial")
  public static class SingletonAssert<T> implements Serializable {

    private final PCollectionView<T> actualView;
    private Optional<Coder<T>> coder;

    protected SingletonAssert(PCollectionView<T> actualView) {
      this.actualView = actualView;
      coder = Optional.absent();
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed
     * for internal purposes.
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
            "Attempting to access the coder of a SingletonAssert"
            + " that has not been set yet.");
      }
    }

    /**
     * Applies a {@link SerializableFunction} to check the value of this
     * {@code SingletonAssert}'s view.
     *
     * <p> Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> satisfies(final SerializableFunction<T, Void> checkerFn) {
      new OneSideInputAssert<T>(actualView).satisfies(checkerFn);
      return this;
    }

    /**
     * Applies an {@link AssertRelation} to check the provided relation against the
     * value of this assert and the provided expected value.
     *
     * <p> Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> satisfies(
        AssertRelation<T, T> relation,
        T expectedValue) {
      new TwoSideInputAssert<T, T>(actualView,
          actualView.getPipeline()
              .apply(Create.of(expectedValue))
              .setCoder(getCoder())
              .apply(View.<T>asSingleton()))
          .satisfies(relation);
      return this;
    }


    /**
     * Checks that the value of this {@code SingletonAssert}'s view is equal
     * to the expected value.
     *
     * <p> Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> isEqualTo(T expectedValue) {
      return satisfies(new AssertIsEqualToRelation<T>(), expectedValue);
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

  /**
   * Returns a new PCollection equivalent to the input, but with all elements
   * in the GlobalWindow.
   */
  private static <T> PCollection<T> inGlobalWindows(PCollection<T> input) {
    return input.apply(Window.<T>into(new GlobalWindows()));
  }

  /**
   * An assertion checker that takes a single {@link PCollectionView PCollectionView&lt;A&gt;}
   * and an assertion over {@code A}, and checks it within a dataflow pipeline.
   *
   * <p> Note that the entire assertion must be serializable. If
   * you need to make assertions involving multiple inputs
   * that are each not serializable, use TwoSideInputAssert.
   *
   * <p> This is generally useful for assertion functions that
   * are serializable but whose underlying data may not have a coder.
   */
  @SuppressWarnings("serial")
  private static class OneSideInputAssert<ActualT> implements Serializable {

    private final PCollectionView<ActualT> actualView;

    public OneSideInputAssert(PCollectionView<ActualT> actualView) {
      this.actualView = actualView;
    }

    public OneSideInputAssert<ActualT> satisfies(
        final SerializableFunction<ActualT, Void> checkerFn) {
      actualView.getPipeline()
        .apply(Create.<Void>of((Void) null))
        .setCoder(VoidCoder.of())
        .apply(ParDo
          .withSideInputs(actualView)
          .of(new DoFn<Void, Void>() {
            @Override
            public void processElement(ProcessContext c) {
              ActualT actualContents = c.sideInput(actualView);
              checkerFn.apply(actualContents);
            }
          }));
      return this;
    }
  }

  /**
   * An assertion checker that takes a {@link PCollectionView PCollectionView&lt;A&gt;},
   * a {@link PCollectionView PCollectionView&lt;B&gt;}, a relation
   * over {@code A} and {@code B}, and checks that the relation holds
   * within a dataflow pipeline.
   *
   * <p> This is useful when either/both of {@code A} and {@code B}
   * are not serializable, but have coders (provided
   * by the underlying {@link PCollection}s).
   */
  @SuppressWarnings("serial")
  private static class TwoSideInputAssert<ActualT, ExpectedT> implements Serializable {

    private final PCollectionView<ActualT> actualView;
    private final PCollectionView<ExpectedT> expectedView;

    protected TwoSideInputAssert(
        PCollectionView<ActualT> actualView,
        PCollectionView<ExpectedT> expectedView) {
      this.actualView = actualView;
      this.expectedView = expectedView;
    }

    public TwoSideInputAssert<ActualT, ExpectedT> satisfies(
        final AssertRelation<ActualT, ExpectedT> relation) {
      actualView.getPipeline()
        .apply(Create.<Void>of((Void) null))
        .setCoder(VoidCoder.of())
        .apply(ParDo
          .withSideInputs(actualView, expectedView)
          .of(new DoFn<Void, Void>() {
            @Override
            public void processElement(ProcessContext c) {
              ActualT actualContents = c.sideInput(actualView);
              ExpectedT expectedContents = c.sideInput(expectedView);
              relation.assertFor(expectedContents).apply(actualContents);
            }
          }));
      return this;
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link SerializableFunction} that verifies that an actual value is equal to an
   * expected value.
   */
  @SuppressWarnings("serial")
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
   * A {@link SerializableFunction} that verifies that an {@code Iterable} contains
   * expected items in any order.
   */
  @SuppressWarnings("serial")
  private static class AssertContainsInAnyOrder<T>
      implements SerializableFunction<Iterable<T>, Void> {

    private T[] expected;

    public AssertContainsInAnyOrder(T... expected) {
      this.expected = expected;
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInAnyOrder(Collection<T> expected) {
      this((T[]) expected.toArray());
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInAnyOrder(Iterable<T> expected) {
      this(Lists.newArrayList(expected));
    }

    @Override
    public Void apply(Iterable<T> actual) {
      assertThat(actual, containsInAnyOrder(expected));
      return null;
    }
  }

  /**
   * A {@link SerializableFunction} that verifies that an {@code Iterable} contains
   * the expected items in the provided order.
   */
  @SuppressWarnings("serial")
  private static class AssertContainsInOrder<T> implements SerializableFunction<Iterable<T>, Void> {
    private T[] expected;

    public AssertContainsInOrder(T... expected) {
      this.expected = expected;
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInOrder(Collection<T> expected) {
      this((T[]) expected.toArray());
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInOrder(Iterable<T> expected) {
      this(Lists.newArrayList(expected));
    }

    @Override
    public Void apply(Iterable<T> actual) {
      assertThat(actual, contains(expected));
      return null;
    }
  }

  ////////////////////////////////////////////////////////////

  /**
   * A binary predicate between types {@code Actual} and {@code Expected}.
   * Implemented as a method {@code assertFor(Expected)} which returns
   * a {@link SerializableFunction SerializableFunction<Actual, Void>}
   * that should verify the assertion..
   */
  public static interface AssertRelation<ActualT, ExpectedT> extends Serializable {
    public SerializableFunction<ActualT, Void> assertFor(ExpectedT input);
  }

  /**
   * An {@link AssertRelation} implementing the binary predicate that two objects are equal.
   */
  private static class AssertIsEqualToRelation<T>
      implements AssertRelation<T, T> {
    private static final long serialVersionUID = 0;

    @Override
    public SerializableFunction<T, Void> assertFor(T expected) {
      return new AssertIsEqualTo<T>(expected);
    }
  }

  /**
   * An {@code AssertRelation} implementing the binary predicate that two collections are equal
   * modulo reordering.
   */
  private static class AssertContainsInAnyOrderRelation<T>
      implements AssertRelation<Iterable<T>, Iterable<T>> {
    private static final long serialVersionUID = 0;

    @Override
    public SerializableFunction<Iterable<T>, Void> assertFor(Iterable<T> expectedElements) {
      return new AssertContainsInAnyOrder<T>(expectedElements);
    }
  }

  /**
   * A {@code AssertRelation} implementating the binary function that two iterables have equal
   * contents, in the same order.
   */
  private static class AssertContainsInOrderRelation<T>
      implements AssertRelation<Iterable<T>, Iterable<T>> {
    private static final long serialVersionUID = 0;

    @Override
    public SerializableFunction<Iterable<T>, Void> assertFor(Iterable<T> expectedElements) {
      return new AssertContainsInOrder<T>(expectedElements);
    }
  }
}
