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

import static org.apache.beam.sdk.transforms.Contextful.fn;
import static org.apache.beam.sdk.transforms.Requirements.requiresSideInputs;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.FlatMapElements.FlatMapWithFailures;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionAsMapHandler;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlatMapElements}. */
@RunWith(JUnit4.class)
public class FlatMapElementsTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  /** Basic test of {@link FlatMapElements} with a {@link SimpleFunction}. */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapSimpleFunction() throws Exception {
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))

            // Note that FlatMapElements takes an InferableFunction<InputT, ? extends
            // Iterable<OutputT>>
            // so the use of List<Integer> here (as opposed to Iterable<Integer>) deliberately
            // exercises
            // the use of an upper bound.
            .apply(
                FlatMapElements.via(
                    new SimpleFunction<Integer, List<Integer>>() {
                      @Override
                      public List<Integer> apply(Integer input) {
                        return ImmutableList.of(-input, input);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder(1, -2, -1, -3, 2, 3);
    pipeline.run();
  }

  /** Basic test of {@link FlatMapElements} with an {@link InferableFunction}. */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapInferableFunction() throws Exception {
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                FlatMapElements.via(
                    new InferableFunction<Integer, List<Integer>>() {
                      @Override
                      public List<Integer> apply(Integer input) throws Exception {
                        return ImmutableList.of(-input, input);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder(1, -2, -1, -3, 2, 3);
    pipeline.run();
  }

  /** Basic test of {@link FlatMapElements} with a {@link Fn} and a side input. */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapBasicWithSideInput() throws Exception {
    final PCollectionView<Integer> view =
        pipeline.apply("Create base", Create.of(40)).apply(View.asSingleton());
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(0, 1, 2))
            .apply(
                FlatMapElements.into(integers())
                    .via(
                        fn(
                            (input, c) ->
                                ImmutableList.of(
                                    c.sideInput(view) - input, c.sideInput(view) + input),
                            requiresSideInputs(view))));

    PAssert.that(output).containsInAnyOrder(38, 39, 40, 40, 41, 42);
    pipeline.run();
  }

  /**
   * Tests that when built with a concrete subclass of {@link SimpleFunction}, the type descriptor
   * of the output reflects its static type.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapFnOutputTypeDescriptor() throws Exception {
    PCollection<String> output =
        pipeline
            .apply(Create.of("hello"))
            .apply(
                FlatMapElements.via(
                    new SimpleFunction<String, Set<String>>() {
                      @Override
                      public Set<String> apply(String input) {
                        return ImmutableSet.copyOf(input.split(""));
                      }
                    }));

    assertThat(
        output.getTypeDescriptor(),
        equalTo((TypeDescriptor<String>) new TypeDescriptor<String>() {}));
    assertThat(
        pipeline.getCoderRegistry().getCoder(output.getTypeDescriptor()),
        equalTo(pipeline.getCoderRegistry().getCoder(new TypeDescriptor<String>() {})));

    // Make sure the pipeline runs
    pipeline.run();
  }

  /**
   * A {@link SimpleFunction} to test that the coder registry can propagate coders that are bound to
   * type variables.
   */
  private static class PolymorphicSimpleFunction<T> extends SimpleFunction<T, Iterable<T>> {
    @Override
    public Iterable<T> apply(T input) {
      return Collections.emptyList();
    }
  }

  /**
   * Basic test of {@link MapElements} coder propagation with a parametric {@link SimpleFunction}.
   */
  @Test
  public void testPolymorphicSimpleFunction() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    pipeline
        .apply(Create.of(1, 2, 3))

        // This is the function that needs to propagate the input T to output T
        .apply("Polymorphic Identity", MapElements.via(new PolymorphicSimpleFunction<>()))

        // This is a consumer to ensure that all coder inference logic is executed.
        .apply(
            "Test Consumer",
            MapElements.via(
                new SimpleFunction<Iterable<Integer>, Integer>() {
                  @Override
                  public Integer apply(Iterable<Integer> input) {
                    return 42;
                  }
                }));
  }

  @Test
  public void testSimpleFunctionClassDisplayData() {
    SimpleFunction<Integer, List<Integer>> simpleFn =
        new SimpleFunction<Integer, List<Integer>>() {
          @Override
          public List<Integer> apply(Integer input) {
            return Collections.emptyList();
          }
        };

    FlatMapElements<?, ?> simpleMap = FlatMapElements.via(simpleFn);
    assertThat(DisplayData.from(simpleMap), hasDisplayItem("class", simpleFn.getClass()));
  }

  @Test
  public void testInferableFunctionClassDisplayData() {
    InferableFunction<Integer, List<Integer>> inferableFn =
        new InferableFunction<Integer, List<Integer>>() {
          @Override
          public List<Integer> apply(Integer input) {
            return Collections.emptyList();
          }
        };

    FlatMapElements<?, ?> inferableMap = FlatMapElements.via(inferableFn);
    assertThat(DisplayData.from(inferableMap), hasDisplayItem("class", inferableFn.getClass()));
  }

  @Test
  public void testSimpleFunctionDisplayData() {
    SimpleFunction<Integer, List<Integer>> simpleFn =
        new SimpleFunction<Integer, List<Integer>>() {
          @Override
          public List<Integer> apply(Integer input) {
            return Collections.emptyList();
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "baz"));
          }
        };

    FlatMapElements<?, ?> simpleFlatMap = FlatMapElements.via(simpleFn);
    assertThat(DisplayData.from(simpleFlatMap), hasDisplayItem("class", simpleFn.getClass()));
    assertThat(DisplayData.from(simpleFlatMap), hasDisplayItem("foo", "baz"));
  }

  @Test
  public void testInferableFunctionDisplayData() {
    InferableFunction<Integer, List<Integer>> inferableFn =
        new InferableFunction<Integer, List<Integer>>() {
          @Override
          public List<Integer> apply(Integer input) {
            return Collections.emptyList();
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "baz"));
          }
        };

    FlatMapElements<?, ?> inferableFlatMap = FlatMapElements.via(inferableFn);
    assertThat(DisplayData.from(inferableFlatMap), hasDisplayItem("class", inferableFn.getClass()));
    assertThat(DisplayData.from(inferableFlatMap), hasDisplayItem("foo", "baz"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testVoidValues() throws Exception {
    pipeline
        .apply(Create.of("hello"))
        .apply(WithKeys.of("k"))
        .apply(new VoidValues<String, String>() {});
    // Make sure the pipeline runs
    pipeline.run();
  }

  static class VoidValues<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Void>>> {

    @Override
    public PCollection<KV<K, Void>> expand(PCollection<KV<K, V>> input) {
      return input.apply(
          FlatMapElements.<KV<K, V>, KV<K, Void>>via(
              new SimpleFunction<KV<K, V>, Iterable<KV<K, Void>>>() {
                @Override
                public Iterable<KV<K, Void>> apply(KV<K, V> input) {
                  return Collections.singletonList(KV.<K, Void>of(input.getKey(), null));
                }
              }));
    }
  }

  /**
   * Basic test of {@link FlatMapElements} with a lambda (which is instantiated as a {@link
   * ProcessFunction}).
   */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapBasicWithLambda() throws Exception {
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                FlatMapElements
                    // Note that the input type annotation is required.
                    .into(TypeDescriptors.integers())
                    .via((Integer i) -> ImmutableList.of(i, -i)));

    PAssert.that(output).containsInAnyOrder(1, 3, -1, -3, 2, -2);
    pipeline.run();
  }

  /** Basic test of {@link FlatMapElements} with a method reference. */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapMethodReference() throws Exception {

    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                FlatMapElements
                    // Note that the input type annotation is required.
                    .into(TypeDescriptors.integers())
                    .via(new Negater()::numAndNegation));

    PAssert.that(output).containsInAnyOrder(1, 3, -1, -3, 2, -2);
    pipeline.run();
  }

  private static class Negater implements Serializable {
    public List<Integer> numAndNegation(int input) {
      return ImmutableList.of(input, -input);
    }
  }

  /** Test of {@link FlatMapWithFailures} with a pre-built exception handler. */
  @Test
  @Category(NeedsRunner.class)
  public void testExceptionAsMap() throws Exception {

    Result<PCollection<Integer>, KV<Integer, Map<String, String>>> result =
        pipeline
            .apply(Create.of(0, 2, 3))
            .apply(
                FlatMapElements.into(TypeDescriptors.integers())
                    .via((Integer i) -> ImmutableList.of(i + 1 / i, -i - 1 / i))
                    .exceptionsVia(new ExceptionAsMapHandler<Integer>() {}));

    PAssert.that(result.output()).containsInAnyOrder(2, -2, 3, -3);
    pipeline.run();
  }

  /** Test of {@link FlatMapWithFailures} with handling defined via lambda expression. */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapWithFailuresLambda() {
    Result<PCollection<Integer>, KV<Integer, String>> result =
        pipeline
            .apply(Create.of(0, 2, 3))
            .apply(
                FlatMapElements.into(TypeDescriptors.integers())
                    .via((Integer i) -> ImmutableList.of(i + 1 / i, -i - 1 / i))
                    .exceptionsInto(
                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                    .exceptionsVia(f -> KV.of(f.element(), f.exception().getMessage())));

    PAssert.that(result.output()).containsInAnyOrder(2, -2, 3, -3);
    PAssert.that(result.failures()).containsInAnyOrder(KV.of(0, "/ by zero"));
    pipeline.run();
  }

  /**
   * Test of {@link FlatMapWithFailures()} with a {@link SimpleFunction} and no {@code into} call.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testFlatMapWithFailuresSimpleFunction() {
    Result<PCollection<Integer>, KV<Integer, String>> result =
        pipeline
            .apply(Create.of(0, 2, 3))
            .apply(
                FlatMapElements.into(TypeDescriptors.integers())
                    .via((Integer i) -> ImmutableList.of(i + 1 / i, -i - 1 / i))
                    .exceptionsVia(
                        new SimpleFunction<ExceptionElement<Integer>, KV<Integer, String>>() {
                          @Override
                          public KV<Integer, String> apply(ExceptionElement<Integer> failure) {
                            return KV.of(failure.element(), failure.exception().getMessage());
                          }
                        }));

    PAssert.that(result.output()).containsInAnyOrder(2, -2, 3, -3);
    PAssert.that(result.failures()).containsInAnyOrder(KV.of(0, "/ by zero"));

    pipeline.run();
  }

  @Test
  public void testFlatMapWithFailuresDisplayData() {
    InferableFunction<Integer, List<Integer>> inferableFn =
        new InferableFunction<Integer, List<Integer>>() {
          @Override
          public List<Integer> apply(Integer input) {
            return ImmutableList.of(input);
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "baz"));
          }
        };

    InferableFunction<ExceptionElement<Integer>, String> exceptionHandler =
        new InferableFunction<ExceptionElement<Integer>, String>() {
          @Override
          public String apply(ExceptionElement<Integer> input) throws Exception {
            return "";
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("bar", "buz"));
          }
        };

    FlatMapWithFailures<?, ?, ?> mapWithFailures =
        FlatMapElements.via(inferableFn).exceptionsVia(exceptionHandler);
    assertThat(DisplayData.from(mapWithFailures), hasDisplayItem("class", inferableFn.getClass()));
    assertThat(
        DisplayData.from(mapWithFailures),
        hasDisplayItem("exceptionHandler.class", exceptionHandler.getClass()));
    assertThat(DisplayData.from(mapWithFailures), hasDisplayItem("foo", "baz"));
    assertThat(DisplayData.from(mapWithFailures), hasDisplayItem("bar", "buz"));
  }
}
