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

package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
   * {@link PCollection PCollection<T>}.
   */
  public static <T> IterableAssert<T> that(PCollection<T> actual) {
    return new IterableAssert<>(actual.apply(View.<T>asIterable()))
        .setCoder(actual.getCoder());
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided
   * {@link PCollection PCollection<Iterable<T>>}, which must be a singleton.
   */
  public static <T> IterableAssert<T> thatSingletonIterable(PCollection<Iterable<T>> actual) {
    List<? extends Coder<?>> maybeElementCoder = actual.getCoder().getCoderArguments();
    Coder<T> tCoder;
    try {
      tCoder = (Coder<T>) Iterables.getOnlyElement(maybeElementCoder);
    } catch (NoSuchElementException | IllegalArgumentException exc) {
      throw new IllegalArgumentException(
        "DataflowAssert.<T>thatSingltonIterable requires a PCollection<Iterable<T>>"
        + " with a Coder<Iterable<T>> where getCoderArguments() yields a"
        + " single Coder<T> to apply to the elements.");
    }

    return new IterableAssert<>(actual.apply(View.<Iterable<T>>asSingleton()))
        .setCoder(tCoder);
  }

  /**
   * Constructs an {@link IterableAssert} for the value of the provided
   * {@code PCollectionView PCollectionView<Iterable<T>, ?>}.
   */
  public static <T> IterableAssert<T> thatIterable(PCollectionView<Iterable<T>, ?> actual) {
    return new IterableAssert<>(actual);
  }

  /**
   * Constructs a {@link SingletonAssert} for the value of the provided
   * {@code PCollection PCollection<T>}, which must be a singleton.
   */
  public static <T> SingletonAssert<T> thatSingleton(PCollection<T> actual) {
    return new SingletonAssert<>(actual.apply(View.<T>asSingleton()))
        .setCoder(actual.getCoder());
  }

  ////////////////////////////////////////////////////////////

  /**
   * An assertion about the contents of a
   * {@link PCollectionView PCollectionView<<Iterable<T>, ?>}.
   */
  @SuppressWarnings("serial")
  public static class IterableAssert<T> implements Serializable {

    private final PCollectionView<Iterable<T>, ?> actualView;
    private Optional<Coder<T>> coder;

    protected IterableAssert(PCollectionView<Iterable<T>, ?> actualView) {
      this.actualView = actualView;
      coder = Optional.absent();
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed
     * for internal purposes.
     */
    public IterableAssert<T> setCoder(Coder<T> coder) {
      this.coder = Optional.of(coder);
      return this;
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed
     * for internal purposes.
     *
     * <p> Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> setCoder(Optional<Coder<T>> coder) {
      this.coder = coder;
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
            + " which has not been set yet.");
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
              .setOrdered(true)
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


    /**
     * Checks that the {@code Iterable} contains the expected elements, in the
     * specified order.
     *
     * <p> Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> containsInOrder(T... expectedElements) {
      return this.satisfies(
          new AssertContainsInOrderRelation<T>(),
          Arrays.asList(expectedElements));
    }

    /**
     * Checks that the {@code Iterable} contains the expected elements, in the
     * specified order.
     *
     * <p> Returns this {@code IterableAssert}.
     */
    public IterableAssert<T> containsInOrder(Iterable<T> expectedElements) {
      return this.satisfies(new AssertContainsInOrderRelation<T>(), expectedElements);
    }
  }

  /**
   * An assertion about the single value of type {@code T}
   * associated with a {@link PCollectionView PCollectionView<T, ?>}.
   */
  @SuppressWarnings("serial")
  public static class SingletonAssert<T> implements Serializable {

    private final PCollectionView<T, ?> actualView;
    private Optional<Coder<T>> coder;

    protected SingletonAssert(PCollectionView<T, ?> actualView) {
      this.actualView = actualView;
      coder = Optional.absent();
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed
     * for internal purposes.
     */
    public SingletonAssert<T> setCoder(Coder<T> coder) {
      this.coder = Optional.of(coder);
      return this;
    }

    /**
     * Sets the coder to use for elements of type {@code T}, as needed
     * for internal purposes.
     *
     * <p> Returns this {@code SingletonAssert}.
     */
    public SingletonAssert<T> setCoder(Optional<Coder<T>> coder) {
      this.coder = coder;
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
            + " which has not been set yet.");
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
              .setOrdered(true)
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

    @Deprecated
    public SingletonAssert<T> is(T expectedValue) {
      return isEqualTo(expectedValue);
    }

  }

  ////////////////////////////////////////////////////////////////////////

  /**
   * An assertion checker that takes a single {@link PCollectionView PCollectionView<A, ?>}
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
  private static class OneSideInputAssert<Actual> implements Serializable {

    private final PCollectionView<Actual, ?> actualView;

    public OneSideInputAssert(PCollectionView<Actual, ?> actualView) {
      this.actualView = actualView;
    }

    public OneSideInputAssert<Actual> satisfies(
        final SerializableFunction<Actual, Void> checkerFn) {
      actualView.getPipeline()
        .apply(Create.<Void>of((Void) null))
        .setCoder(VoidCoder.of())
        .apply(ParDo
          .withSideInputs(actualView)
          .of(new DoFn<Void, Void>() {
            @Override
            public void processElement(ProcessContext c) {
              Actual actualContents = c.sideInput(actualView);
              checkerFn.apply(actualContents);
            }
          }));
      return this;
    }
  }

  /**
   * An assertion checker that takes a {@link PCollectionView PCollectionView<A, ?>},
   * a {@link PCollectionView PCollectionView<B, ?>}, a relation
   * over {@code A} and {@code B}, and checks that the relation holds
   * within a dataflow pipeline.
   *
   * <p> This is useful when either/both of {@code A} and {@code B}
   * are not serializable, but have coders (provided
   * by the underlying {@link PCollection}s).
   */
  @SuppressWarnings("serial")
  private static class TwoSideInputAssert<Actual, Expected> implements Serializable {

    private final PCollectionView<Actual, ?> actualView;
    private final PCollectionView<Expected, ?> expectedView;

    protected TwoSideInputAssert(
        PCollectionView<Actual, ?> actualView,
        PCollectionView<Expected, ?> expectedView) {
      this.actualView = actualView;
      this.expectedView = expectedView;
    }

    public TwoSideInputAssert<Actual, Expected> satisfies(
        final AssertRelation<Actual, Expected> relation) {
      actualView.getPipeline()
        .apply(Create.<Void>of((Void) null))
        .setCoder(VoidCoder.of())
        .apply(ParDo
          .withSideInputs(actualView, expectedView)
          .of(new DoFn<Void, Void>() {
            @Override
            public void processElement(ProcessContext c) {
              Actual actualContents = c.sideInput(actualView);
              Expected expectedContents = c.sideInput(expectedView);
              relation.assertFor(expectedContents).apply(actualContents);
            }
          }));
      return this;
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link SerializableFunction} that performs an
   * {@code Assert.assertThat()} operation using a
   * {@code Matcher} operation.
   *
   * <P> The {@code MatcherFactory} should take an {@code Expected} and
   * produce a Matcher to be used to check an {@code Actual} value
   * against.
   */
  @SuppressWarnings("serial")
  public static class AssertThat<Actual, Expected>
      implements SerializableFunction<Actual, Void> {
    final Expected expected;
    final Class<?> expectedClass;
    final String matcherClassName;
    final String matcherFactoryMethodName;

    AssertThat(Expected expected,
               Class<?> expectedClass,
               String matcherClassName,
               String matcherFactoryMethodName) {
      this.expected = expected;
      this.expectedClass = expectedClass;
      this.matcherClassName = matcherClassName;
      this.matcherFactoryMethodName = matcherFactoryMethodName;
    }

    @Override
    public Void apply(Actual in) {
      try {
        Method matcherFactoryMethod = Class.forName(this.matcherClassName)
            .getMethod(this.matcherFactoryMethodName, expectedClass);
        Object matcher = matcherFactoryMethod.invoke(null, (Object) expected);
        Method assertThatMethod = Class.forName("org.junit.Assert")
            .getMethod("assertThat",
                       Object.class,
                       Class.forName("org.hamcrest.Matcher"));
        assertThatMethod.invoke(null, in, matcher);
      } catch (InvocationTargetException e) {
        // An error in the assertThat or matcher itself.
        throw new RuntimeException(e);
      } catch (ReflectiveOperationException e) {
        // An error looking up the classes and methods.
        throw new RuntimeException(
            "DataflowAssert requires that JUnit and Hamcrest be linked in.",
            e);
      }
      return null;
    }
  }

  /**
   * An {@link AssertThat} taking a single element.
   */
  @SuppressWarnings("serial")
  private static class AssertThatValue<T> extends AssertThat<T, T> {
    AssertThatValue(T expected,
                    String matcherClassName,
                    String matcherFactoryMethodName) {
      super(expected, Object.class,
            matcherClassName, matcherFactoryMethodName);
    }
  }

  /**
   * An {@link AssertThatValue} that verifies that an actual value is equal to an
   * expected value.
   */
  @SuppressWarnings("serial")
  private static class AssertIsEqualTo<T> extends AssertThatValue<T> {
    public AssertIsEqualTo(T expected) {
      super(expected, "org.hamcrest.core.IsEqual", "equalTo");
    }
  }

  /**
   * An {@link AssertThat} that operates on an {@code Iterable}. The
   * underlying matcher takes a {@code T[]} of expected values, for
   * compatibility with the corresponding Hamcrest {@code Matcher}s.
   */
  @SuppressWarnings("serial")
  private static class AssertThatIterable<T> extends AssertThat<Iterable<T>, T[]> {
    AssertThatIterable(T[] expected,
                       String matcherClassName,
                       String matcherFactoryMethodName) {
      super(expected, Object[].class,
            matcherClassName, matcherFactoryMethodName);
    }
  }

  /**
   * An {@link AssertThatIterable} that verifies that an {@code Iterable} contains
   * expected items in any order.
   */
  @SuppressWarnings("serial")
  private static class AssertContainsInAnyOrder<T> extends AssertThatIterable<T> {
    public AssertContainsInAnyOrder(T... expected) {
      super(expected,
            "org.hamcrest.collection.IsIterableContainingInAnyOrder",
            "containsInAnyOrder");
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInAnyOrder(Collection<T> expected) {
      this((T[]) expected.toArray());
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInAnyOrder(Iterable<T> expected) {
      this(Lists.newArrayList(expected));
    }
  }

  /**
   * An {@link AssertThatIterable} that verifies that an {@code Iterable} contains
   * the expected items in the provided order.
   */
  @SuppressWarnings("serial")
  private static class AssertContainsInOrder<T> extends AssertThatIterable<T> {
    public AssertContainsInOrder(T... expected) {
      super(expected,
            "org.hamcrest.collection.IsIterableContainingInOrder",
            "contains");
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInOrder(Collection<T> expected) {
      this((T[]) expected.toArray());
    }

    @SuppressWarnings("unchecked")
    public AssertContainsInOrder(Iterable<T> expected) {
      this(Lists.newArrayList(expected));
    }
  }

  ////////////////////////////////////////////////////////////

  /**
   * A serializable function implementing a binary predicate
   * between types {@code Actual} and {@code Expected}.
   */
  public static interface AssertRelation<Actual, Expected> extends Serializable {
    public SerializableFunction<Actual, Void> assertFor(Expected input);
  }

  /**
   * An {@link AssertRelation} implementing the binary predicate
   * that two objects are equal.
   */
  private static class AssertIsEqualToRelation<T>
      implements AssertRelation<T, T> {

    @Override
    public AssertThat<T, T> assertFor(T expected) {
      return new AssertIsEqualTo<T>(expected);
    }
  }

  /**
   * An {@code AssertRelation} implementing the binary predicate
   * that two collections are equal modulo reordering.
   */
  private static class AssertContainsInAnyOrderRelation<T>
      implements AssertRelation<Iterable<T>, Iterable<T>> {

    @Override
    public SerializableFunction<Iterable<T>, Void> assertFor(Iterable<T> expectedElements) {
      return new AssertContainsInAnyOrder<T>(expectedElements);
    }
  }

  /**
   * A {@code AssertRelation} implementating the binary function
   * that two iterables have equal contents, in the same order.
   */
  private static class AssertContainsInOrderRelation<T>
      implements AssertRelation<Iterable<T>, Iterable<T>> {

    @Override
    public SerializableFunction<Iterable<T>, Void> assertFor(Iterable<T> expectedElements) {
      return new AssertContainsInOrder<T>(expectedElements);
    }
  }
}
