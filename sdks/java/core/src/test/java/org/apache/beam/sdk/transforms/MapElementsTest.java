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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.MapElements.MapWithFailures;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MapElements}. */
@RunWith(JUnit4.class)
public class MapElementsTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);

  /**
   * A {@link SimpleFunction} to test that the coder registry can propagate coders that are bound to
   * type variables.
   */
  private static class PolymorphicSimpleFunction<T> extends SimpleFunction<T, T> {
    @Override
    public T apply(T input) {
      return input;
    }
  }

  /**
   * A {@link SimpleFunction} to test that the coder registry can propagate coders that are bound to
   * type variables, when the variable appears nested in the output.
   */
  private static class NestedPolymorphicSimpleFunction<T> extends SimpleFunction<T, KV<T, String>> {
    @Override
    public KV<T, String> apply(T input) {
      return KV.of(input, "hello");
    }
  }

  /**
   * An {@link InferableFunction} to test that the coder registry can propagate coders that are
   * bound to type variables.
   */
  private static class PolymorphicInferableFunction<T> extends InferableFunction<T, T> {
    @Override
    public T apply(T input) throws Exception {
      return input;
    }
  }

  /**
   * An {@link InferableFunction} to test that the coder registry can propagate coders that are
   * bound to type variables, when the variable appears nested in the output.
   */
  private static class NestedPolymorphicInferableFunction<T>
      extends InferableFunction<T, KV<T, String>> {
    @Override
    public KV<T, String> apply(T input) throws Exception {
      return KV.of(input, "hello");
    }
  }

  /** Basic test of {@link MapElements} with a {@link SimpleFunction}. */
  @Test
  @Category(NeedsRunner.class)
  public void testMapSimpleFunction() throws Exception {
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                MapElements.via(
                    new SimpleFunction<Integer, Integer>() {
                      @Override
                      public Integer apply(Integer input) {
                        return -input;
                      }
                    }));

    PAssert.that(output).containsInAnyOrder(-2, -1, -3);
    pipeline.run();
  }

  /** Basic test of {@link MapElements} with an {@link InferableFunction}. */
  @Test
  @Category(NeedsRunner.class)
  public void testMapInferableFunction() throws Exception {
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                MapElements.via(
                    new InferableFunction<Integer, Integer>() {
                      @Override
                      public Integer apply(Integer input) throws Exception {
                        return -input;
                      }
                    }));

    PAssert.that(output).containsInAnyOrder(-2, -1, -3);
    pipeline.run();
  }

  /** Basic test of {@link MapElements} with a {@link Fn} and a side input. */
  @Test
  @Category(NeedsRunner.class)
  public void testMapBasicWithSideInput() throws Exception {
    final PCollectionView<Integer> view =
        pipeline.apply("Create base", Create.of(40)).apply(View.asSingleton());
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(0, 1, 2))
            .apply(
                MapElements.into(integers())
                    .via(
                        fn((element, c) -> element + c.sideInput(view), requiresSideInputs(view))));

    PAssert.that(output).containsInAnyOrder(40, 41, 42);
    pipeline.run();
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
                new SimpleFunction<Integer, Integer>() {
                  @Override
                  public Integer apply(Integer input) {
                    return input;
                  }
                }));
  }

  /**
   * Basic test of {@link MapElements} coder propagation with a parametric {@link
   * InferableFunction}.
   */
  @Test
  public void testPolymorphicInferableFunction() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    pipeline
        .apply(Create.of(1, 2, 3))
        .apply("Polymorphic Identity", MapElements.via(new PolymorphicInferableFunction<>()))
        .apply(
            "Test Consumer",
            MapElements.via(
                new InferableFunction<Integer, Integer>() {
                  @Override
                  public Integer apply(Integer input) throws Exception {
                    return input;
                  }
                }));
  }

  /**
   * Test of {@link MapElements} coder propagation with a parametric {@link SimpleFunction} where
   * the type variable occurs nested within other concrete type constructors.
   */
  @Test
  public void testNestedPolymorphicSimpleFunction() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    pipeline
        .apply(Create.of(1, 2, 3))

        // This is the function that needs to propagate the input T to output T
        .apply("Polymorphic Identity", MapElements.via(new NestedPolymorphicSimpleFunction<>()))

        // This is a consumer to ensure that all coder inference logic is executed.
        .apply(
            "Test Consumer",
            MapElements.via(
                new SimpleFunction<KV<Integer, String>, Integer>() {
                  @Override
                  public Integer apply(KV<Integer, String> input) {
                    return 42;
                  }
                }));
  }

  /**
   * Test of {@link MapElements} coder propagation with a parametric {@link InferableFunction} where
   * the type variable occurs nested within other concrete type constructors.
   */
  @Test
  public void testNestedPolymorphicInferableFunction() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    pipeline
        .apply(Create.of(1, 2, 3))
        .apply("Polymorphic Identity", MapElements.via(new NestedPolymorphicInferableFunction<>()))
        .apply(
            "Test Consumer",
            MapElements.via(
                new InferableFunction<KV<Integer, String>, Integer>() {
                  @Override
                  public Integer apply(KV<Integer, String> input) throws Exception {
                    return 42;
                  }
                }));
  }

  /** Basic test of {@link MapElements} with a {@link ProcessFunction}. */
  @Test
  @Category(NeedsRunner.class)
  public void testMapBasicProcessFunction() throws Exception {
    PCollection<Integer> output =
        pipeline.apply(Create.of(1, 2, 3)).apply(MapElements.into(integers()).via(input -> -input));

    PAssert.that(output).containsInAnyOrder(-2, -1, -3);
    pipeline.run();
  }

  /**
   * Tests that when built with a concrete subclass of {@link SimpleFunction}, the type descriptor
   * of the output reflects its static type.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testSimpleFunctionOutputTypeDescriptor() throws Exception {
    PCollection<String> output =
        pipeline
            .apply(Create.of("hello"))
            .apply(
                MapElements.via(
                    new SimpleFunction<String, String>() {
                      @Override
                      public String apply(String input) {
                        return input;
                      }
                    }));
    assertThat(
        output.getTypeDescriptor(),
        equalTo((TypeDescriptor<String>) new TypeDescriptor<String>() {}));
    assertThat(
        pipeline.getCoderRegistry().getCoder(output.getTypeDescriptor()),
        equalTo(pipeline.getCoderRegistry().getCoder(new TypeDescriptor<String>() {})));

    // Make sure the pipeline runs too
    pipeline.run();
  }

  /**
   * Tests that when built with a concrete subclass of {@link InferableFunction}, the type
   * descriptor of the output reflects its static type.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testInferableFunctionOutputTypeDescriptor() throws Exception {
    PCollection<String> output =
        pipeline
            .apply(Create.of("hello"))
            .apply(
                MapElements.via(
                    new InferableFunction<String, String>() {
                      @Override
                      public String apply(String input) throws Exception {
                        return input;
                      }
                    }));
    assertThat(
        output.getTypeDescriptor(),
        equalTo((TypeDescriptor<String>) new TypeDescriptor<String>() {}));
    assertThat(
        pipeline.getCoderRegistry().getCoder(output.getTypeDescriptor()),
        equalTo(pipeline.getCoderRegistry().getCoder(new TypeDescriptor<String>() {})));

    // Make sure the pipeline runs too
    pipeline.run();
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

  @Test
  public void testSerializableFunctionDisplayData() {
    SerializableFunction<Integer, Integer> serializableFn = input -> input;

    MapElements<?, ?> serializableMap = MapElements.into(integers()).via(serializableFn);
    assertThat(
        DisplayData.from(serializableMap), hasDisplayItem("class", serializableFn.getClass()));
  }

  @Test
  public void testProcessFunctionDisplayData() {
    ProcessFunction<Integer, Integer> processFn = input -> input;

    MapElements<?, ?> processMap = MapElements.into(integers()).via(processFn);
    assertThat(DisplayData.from(processMap), hasDisplayItem("class", processFn.getClass()));
  }

  @Test
  public void testSimpleFunctionClassDisplayData() {
    SimpleFunction<?, ?> simpleFn =
        new SimpleFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return input;
          }
        };

    MapElements<?, ?> simpleMap = MapElements.via(simpleFn);
    assertThat(DisplayData.from(simpleMap), hasDisplayItem("class", simpleFn.getClass()));
  }

  @Test
  public void testInferableFunctionClassDisplayData() {
    InferableFunction<?, ?> inferableFn =
        new InferableFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) throws Exception {
            return input;
          }
        };

    MapElements<?, ?> inferableMap = MapElements.via(inferableFn);
    assertThat(DisplayData.from(inferableMap), hasDisplayItem("class", inferableFn.getClass()));
  }

  @Test
  public void testSimpleFunctionDisplayData() {
    SimpleFunction<Integer, ?> simpleFn =
        new SimpleFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return input;
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "baz"));
          }
        };

    MapElements<?, ?> simpleMap = MapElements.via(simpleFn);
    assertThat(DisplayData.from(simpleMap), hasDisplayItem("class", simpleFn.getClass()));
    assertThat(DisplayData.from(simpleMap), hasDisplayItem("foo", "baz"));
  }

  @Test
  public void testInferableFunctionDisplayData() {
    InferableFunction<Integer, ?> inferableFn =
        new InferableFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return input;
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "baz"));
          }
        };

    MapElements<?, ?> inferableMap = MapElements.via(inferableFn);
    assertThat(DisplayData.from(inferableMap), hasDisplayItem("class", inferableFn.getClass()));
    assertThat(DisplayData.from(inferableMap), hasDisplayItem("foo", "baz"));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPrimitiveDisplayData() {
    SimpleFunction<Integer, ?> mapFn =
        new SimpleFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return input;
          }
        };

    MapElements<Integer, ?> map = MapElements.via(mapFn);
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(map);
    assertThat(
        "MapElements should include the mapFn in its primitive display data",
        displayData,
        hasItem(hasDisplayItem("class", mapFn.getClass())));
  }

  static class VoidValues<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Void>>> {

    @Override
    public PCollection<KV<K, Void>> expand(PCollection<KV<K, V>> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<KV<K, V>, KV<K, Void>>() {
                @Override
                public KV<K, Void> apply(KV<K, V> input) {
                  return KV.of(input.getKey(), null);
                }
              }));
    }
  }

  /**
   * Basic test of {@link MapElements} with a lambda (which is instantiated as a {@link
   * SerializableFunction}).
   */
  @Test
  @Category(NeedsRunner.class)
  public void testMapLambda() throws Exception {

    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                MapElements
                    // Note that the type annotation is required.
                    .into(TypeDescriptors.integers())
                    .via((Integer i) -> i * 2));

    PAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  /**
   * Basic test of {@link MapElements} with a lambda wrapped into a {@link SimpleFunction} to
   * remember its type.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testMapWrappedLambda() throws Exception {

    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(MapElements.via(new SimpleFunction<Integer, Integer>((Integer i) -> i * 2) {}));

    PAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  /** Basic test of {@link MapElements} with a method reference. */
  @Test
  @Category(NeedsRunner.class)
  public void testMapMethodReference() throws Exception {

    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                MapElements
                    // Note that the type annotation is required.
                    .into(TypeDescriptors.integers())
                    .via(new Doubler()::doubleIt));

    PAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  private static class Doubler implements Serializable {
    public int doubleIt(int val) {
      return val * 2;
    }
  }

  /** Test of {@link MapWithFailures()} using a pre-built exception handler. */
  @Test
  @Category(NeedsRunner.class)
  public void testExceptionAsMap() {
    Result<PCollection<Integer>, KV<Integer, Map<String, String>>> result =
        pipeline
            .apply(Create.of(0, 1))
            .apply(
                MapElements.into(TypeDescriptors.integers())
                    .via((Integer i) -> 1 / i)
                    .exceptionsVia(new WithFailures.ExceptionAsMapHandler<Integer>() {}));

    PAssert.that(result.output()).containsInAnyOrder(1);

    PAssert.thatSingleton(result.failures())
        .satisfies(
            kv -> {
              assertEquals(Integer.valueOf(0), kv.getKey());
              assertThat(kv.getValue().entrySet(), hasSize(3));
              assertThat(kv.getValue(), hasKey("stackTrace"));
              assertEquals("java.lang.ArithmeticException", kv.getValue().get("className"));
              assertEquals("/ by zero", kv.getValue().get("message"));
              return null;
            });

    pipeline.run();
  }

  /** Test of {@link MapWithFailures()} with handling defined via lambda expression. */
  @Test
  @Category(NeedsRunner.class)
  public void testMapWithFailuresLambda() {
    Result<PCollection<Integer>, KV<Integer, String>> result =
        pipeline
            .apply(Create.of(0, 1))
            .apply(
                MapElements.into(TypeDescriptors.integers())
                    .via((Integer i) -> 1 / i)
                    .exceptionsInto(
                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                    .exceptionsVia(f -> KV.of(f.element(), f.exception().getMessage())));

    PAssert.that(result.output()).containsInAnyOrder(1);

    PAssert.that(result.failures()).containsInAnyOrder(KV.of(0, "/ by zero"));

    pipeline.run();
  }

  /** Test of {@link MapWithFailures()} with a {@link SimpleFunction} and no {@code into} call. */
  @Test
  @Category(NeedsRunner.class)
  public void testMapWithFailuresSimpleFunction() {
    Result<PCollection<Integer>, KV<Integer, String>> result =
        pipeline
            .apply(Create.of(0, 1))
            .apply(
                MapElements.into(TypeDescriptors.integers())
                    .via((Integer i) -> 1 / i)
                    .exceptionsVia(
                        new SimpleFunction<ExceptionElement<Integer>, KV<Integer, String>>() {
                          @Override
                          public KV<Integer, String> apply(ExceptionElement<Integer> failure) {
                            return KV.of(failure.element(), failure.exception().getMessage());
                          }
                        }));

    PAssert.that(result.output()).containsInAnyOrder(1);

    PAssert.that(result.failures()).containsInAnyOrder(KV.of(0, "/ by zero"));

    pipeline.run();
  }

  @Test
  public void testMapWithFailuresDisplayData() {
    InferableFunction<Integer, Integer> inferableFn =
        new InferableFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return input;
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

    MapWithFailures<?, ?, ?> mapWithFailures =
        MapElements.via(inferableFn).exceptionsVia(exceptionHandler);
    assertThat(DisplayData.from(mapWithFailures), hasDisplayItem("class", inferableFn.getClass()));
    assertThat(
        DisplayData.from(mapWithFailures),
        hasDisplayItem("exceptionHandler.class", exceptionHandler.getClass()));
    assertThat(DisplayData.from(mapWithFailures), hasDisplayItem("foo", "baz"));
    assertThat(DisplayData.from(mapWithFailures), hasDisplayItem("bar", "buz"));
  }
}
