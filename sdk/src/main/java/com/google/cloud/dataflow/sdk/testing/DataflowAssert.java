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

import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

/**
 * An assertion on the contents of a {@link PCollection}
 * incorporated into the pipeline.  Such an assertion
 * can be checked no matter what kind of
 * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner} is
 * used, so it's good for testing using the
 * {@link com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner},
 * the
 * {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner},
 * etc.
 *
 * <p>Note that the {@code DataflowAssert} call must precede the call
 * to {@link com.google.cloud.dataflow.sdk.Pipeline#run}.
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
 *
 * @param <T> The type of elements in the input collection.
 */
public class DataflowAssert<T> {
  /**
   * Constructs an IterableAssert for the elements of the provided
   * {@code PCollection<T>}.
   */
  public static <T> IterableAssert<T> that(PCollection<T> futureResult) {
    return new IterableAssert<>(futureResult.apply(View.<T>asIterable()));
  }

  /**
   * Constructs an IterableAssert for the value of the provided
   * {@code PCollection<Iterable<T>>}, which must be a singleton.
   */
  public static <T> IterableAssert<T> thatSingletonIterable(
      PCollection<Iterable<T>> futureResult) {
    return new IterableAssert<>(futureResult.apply(View.<Iterable<T>>asSingleton()));
  }

  /**
   * Constructs an IterableAssert for the value of the provided
   * {@code PCollectionView<Iterable<T>, ?>}.
   */
  public static <T> IterableAssert<T> thatIterable(
      PCollectionView<Iterable<T>, ?> futureResult) {
    return new IterableAssert<>(futureResult);
  }

  /**
   * An assertion about the contents of a {@link PCollectionView<<Iterable<T>, ?>}
   */
  @SuppressWarnings("serial")
  public static class IterableAssert<T> implements Serializable {
    private final PCollectionView<Iterable<T>, ?> actualResults;

    private IterableAssert(PCollectionView<Iterable<T>, ?> futureResult) {
      actualResults = futureResult;
    }

    /**
     * Applies a SerializableFunction to check the elements of the Iterable.
     *
     * <p> Returns this IterableAssert.
     */
    public IterableAssert<T> satisfies(
        final SerializableFunction<Iterable<T>, Void> checkerFn) {

      actualResults.getPipeline()
          .apply(Create.<Void>of((Void) null))
          .setCoder(VoidCoder.of())
          .apply(ParDo
            .withSideInputs(actualResults)
            .of(new DoFn<Void, Void>() {
              @Override
              public void processElement(ProcessContext c) {
                Iterable<T> actualContents = c.sideInput(actualResults);
                checkerFn.apply(actualContents);
              }
            }));

      return this;
    }

    /**
     * Checks that the Iterable contains the expected elements, in any
     * order.
     *
     * <p> Returns this IterableAssert.
     */
    public IterableAssert<T> containsInAnyOrder(T... expectedElements) {
      return this.satisfies(new AssertContainsInAnyOrder<T>(expectedElements));
    }

    /**
     * Checks that the Iterable contains the expected elements, in any
     * order.
     *
     * <p> Returns this IterableAssert.
     */
    public IterableAssert<T> containsInAnyOrder(
        Collection<T> expectedElements) {
      return this.satisfies(new AssertContainsInAnyOrder<T>(expectedElements));
    }

    /**
     * Checks that the Iterable contains the expected elements, in the
     * specified order.
     *
     * <p> Returns this IterableAssert.
     */
    public IterableAssert<T> containsInOrder(T... expectedElements) {
      return this.satisfies(new AssertContainsInOrder<T>(expectedElements));
    }

    /**
     * Checks that the Iterable contains the expected elements, in the
     * specified order.
     *
     * <p> Returns this IterableAssert.
     */
    public IterableAssert<T> containsInOrder(Collection<T> expectedElements) {
      return this.satisfies(new AssertContainsInOrder<T>(expectedElements));
    }

    /**
     * SerializableFunction that performs an {@code Assert.assertThat()}
     * operation using a {@code Matcher} operation that takes an array
     * of elements.
     */
    @SuppressWarnings("serial")
    static class AssertThatIterable<T> extends AssertThat<Iterable<T>, T[]> {
      AssertThatIterable(T[] expected,
                         String matcherClassName,
                         String matcherFactoryMethodName) {
        super(expected, Object[].class,
              matcherClassName, matcherFactoryMethodName);
      }
    }

    /**
     * SerializableFunction that verifies that an Iterable contains
     * expected items in any order.
     */
    @SuppressWarnings("serial")
    static class AssertContainsInAnyOrder<T> extends AssertThatIterable<T> {
      AssertContainsInAnyOrder(T... expected) {
        super(expected,
              "org.hamcrest.collection.IsIterableContainingInAnyOrder",
              "containsInAnyOrder");
      }
      @SuppressWarnings("unchecked")
      AssertContainsInAnyOrder(Collection<T> expected) {
        this((T[]) expected.toArray());
      }
    }

    /**
     * SerializableFunction that verifies that an Iterable contains
     * expected items in the provided order.
     */
    @SuppressWarnings("serial")
    static class AssertContainsInOrder<T> extends AssertThatIterable<T> {
      AssertContainsInOrder(T... expected) {
        super(expected,
              "org.hamcrest.collection.IsIterableContainingInOrder",
              "contains");
      }
      @SuppressWarnings("unchecked")
      AssertContainsInOrder(Collection<T> expected) {
        this((T[]) expected.toArray());
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Constructs a SingletonAssert for the value of the provided
   * {@code PCollection<T>}, which must be a singleton.
   */
  public static <T> SingletonAssert<T> thatSingleton(PCollection<T> futureResult) {
    return new SingletonAssert<>(futureResult.apply(View.<T>asSingleton()));
  }

  /**
   * An assertion about a single value.
   */
  @SuppressWarnings("serial")
  public static class SingletonAssert<T> implements Serializable {
    private final PCollectionView<T, ?> actualResult;

    private SingletonAssert(PCollectionView<T, ?> futureResult) {
      actualResult = futureResult;
    }

    /**
     * Applies a SerializableFunction to check the value of this
     * SingletonAssert's view.
     *
     * <p> Returns this SingletonAssert.
     */
    public SingletonAssert<T> satisfies(final SerializableFunction<T, Void> checkerFn) {
      actualResult.getPipeline()
          .apply(Create.<Void>of((Void) null))
          .setCoder(VoidCoder.of())
          .apply(ParDo
            .withSideInputs(actualResult)
            .of(new DoFn<Void, Void>() {
              @Override
              public void processElement(ProcessContext c) {
                T actualContents = c.sideInput(actualResult);
                checkerFn.apply(actualContents);
              }
            }));

      return this;
    }

    /**
     * Checks that the value of this SingletonAssert's view is equal
     * to the expected value.
     *
     * <p> Returns this SingletonAssert.
     */
    public SingletonAssert<T> is(T expectedValue) {
      return this.satisfies(new AssertIs<T>(expectedValue));
    }

    /**
     * SerializableFunction that performs an {@code Assert.assertThat()}
     * operation using a {@code Matcher} operation that takes a single element.
     */
    @SuppressWarnings("serial")
    static class AssertThatValue<T> extends AssertThat<T, T> {
      AssertThatValue(T expected,
                      String matcherClassName,
                      String matcherFactoryMethodName) {
        super(expected, Object.class,
              matcherClassName, matcherFactoryMethodName);
      }
    }

    /**
     * SerializableFunction that verifies that a value is equal to an
     * expected value.
     */
    @SuppressWarnings("serial")
    public static class AssertIs<T> extends AssertThatValue<T> {
      AssertIs(T expected) {
        super(expected, "org.hamcrest.core.IsEqual", "equalTo");
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////


  // Do not instantiate.
  private DataflowAssert() {}

  /**
   * SerializableFunction that performs an {@code Assert.assertThat()}
   * operation using a {@code Matcher} operation.
   *
   * <P> The MatcherFactory should take an {@code Expected} and
   * produce a Matcher to be used to check an {@code Actual} value
   * against.
   */
  @SuppressWarnings("serial")
  public static class AssertThat<Actual, Expected>
      implements SerializableFunction<Actual, Void> {
    final Expected expected;
    final Class expectedClass;
    final String matcherClassName;
    final String matcherFactoryMethodName;

    AssertThat(Expected expected,
               Class expectedClass,
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
   * SerializableFunction that performs an {@code Assert.assertThat()}
   * operation using a {@code Matcher} operation that takes a single element.
   */
  @SuppressWarnings("serial")
  static class AssertThatValue<T> extends AssertThat<T, T> {
    AssertThatValue(T expected,
                    String matcherClassName,
                    String matcherFactoryMethodName) {
      super(expected, Object.class,
            matcherClassName, matcherFactoryMethodName);
    }
  }

  /**
   * SerializableFunction that verifies that a value is equal to an
   * expected value.
   */
  @SuppressWarnings("serial")
  public static class AssertIs<T> extends AssertThatValue<T> {
    public AssertIs(T expected) {
      super(expected, "org.hamcrest.core.IsEqual", "equalTo");
    }
  }
}
