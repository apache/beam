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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@code PTransform}s for mapping a simple function over the elements of a {@link PCollection}. */
public class MapElements<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  private final transient @Nullable TypeDescriptor<InputT> inputType;
  private final transient @Nullable TypeDescriptor<OutputT> outputType;
  private final transient @Nullable Object originalFnForDisplayData;
  private final @Nullable Contextful<Fn<InputT, OutputT>> fn;

  private MapElements(
      @Nullable Contextful<Fn<InputT, OutputT>> fn,
      @Nullable Object originalFnForDisplayData,
      @Nullable TypeDescriptor<InputT> inputType,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
    this.originalFnForDisplayData = originalFnForDisplayData;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  /**
   * For {@code InferableFunction<InputT, OutputT>} {@code fn}, returns a {@code PTransform} that
   * takes an input {@code PCollection<InputT>} and returns a {@code PCollection<OutputT>}
   * containing {@code fn.apply(v)} for every element {@code v} in the input.
   *
   * <p>{@link InferableFunction} has the advantage of providing type descriptor information, but it
   * is generally more convenient to specify output type via {@link #into(TypeDescriptor)}, and
   * provide the mapping as a lambda expression to {@link #via(ProcessFunction)}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<Integer> wordsPerLine = words.apply(MapElements.via(
   *     new InferableFunction<String, Integer>() {
   *       public Integer apply(String word) throws Exception {
   *         return word.length();
   *       }
   *     }));
   * }</pre>
   */
  public static <InputT, OutputT> MapElements<InputT, OutputT> via(
      final InferableFunction<InputT, OutputT> fn) {
    return new MapElements<>(
        Contextful.fn(fn), fn, fn.getInputTypeDescriptor(), fn.getOutputTypeDescriptor());
  }

  /** Binary compatibility adapter for {@link #via(InferableFunction)}. */
  public static <InputT, OutputT> MapElements<InputT, OutputT> via(
      final SimpleFunction<InputT, OutputT> fn) {
    return via((InferableFunction<InputT, OutputT>) fn);
  }

  /**
   * Returns a new {@link MapElements} transform with the given type descriptor for the output type,
   * but the mapping function yet to be specified using {@link #via(ProcessFunction)}.
   */
  public static <OutputT> MapElements<?, OutputT> into(final TypeDescriptor<OutputT> outputType) {
    return new MapElements<>(null, null, null, outputType);
  }

  /**
   * For a {@code ProcessFunction<InputT, OutputT>} {@code fn} and output type descriptor, returns a
   * {@code PTransform} that takes an input {@code PCollection<InputT>} and returns a {@code
   * PCollection<OutputT>} containing {@code fn.apply(v)} for every element {@code v} in the input.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<Integer> wordLengths = words.apply(
   *     MapElements.into(TypeDescriptors.integers())
   *                .via((String word) -> word.length()));
   * }</pre>
   */
  public <NewInputT> MapElements<NewInputT, OutputT> via(ProcessFunction<NewInputT, OutputT> fn) {
    return new MapElements<>(Contextful.fn(fn), fn, TypeDescriptors.inputOf(fn), outputType);
  }

  /** Binary compatibility adapter for {@link #via(ProcessFunction)}. */
  public <NewInputT> MapElements<NewInputT, OutputT> via(
      SerializableFunction<NewInputT, OutputT> fn) {
    return via((ProcessFunction<NewInputT, OutputT>) fn);
  }

  /** Like {@link #via(ProcessFunction)}, but supports access to context, such as side inputs. */
  @Experimental(Kind.CONTEXTFUL)
  public <NewInputT> MapElements<NewInputT, OutputT> via(Contextful<Fn<NewInputT, OutputT>> fn) {
    return new MapElements<>(
        fn, fn.getClosure(), TypeDescriptors.inputOf(fn.getClosure()), outputType);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkNotNull(fn, "Must specify a function on MapElements using .via()");
    return input.apply(
        "Map",
        ParDo.of(
                new DoFn<InputT, OutputT>() {
                  @ProcessElement
                  public void processElement(
                      @Element InputT element, OutputReceiver<OutputT> receiver, ProcessContext c)
                      throws Exception {
                    receiver.output(
                        fn.getClosure().apply(element, Fn.Context.wrapProcessContext(c)));
                  }

                  @Override
                  public void populateDisplayData(DisplayData.Builder builder) {
                    builder.delegate(MapElements.this);
                  }

                  @Override
                  public TypeDescriptor<InputT> getInputTypeDescriptor() {
                    return inputType;
                  }

                  @Override
                  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
                    checkState(
                        outputType != null,
                        "%s output type descriptor was null; "
                            + "this probably means that getOutputTypeDescriptor() was called after "
                            + "serialization/deserialization, but it is only available prior to "
                            + "serialization, for constructing a pipeline and inferring coders",
                        MapElements.class.getSimpleName());
                    return outputType;
                  }
                })
            .withSideInputs(fn.getRequirements().getSideInputs()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("class", originalFnForDisplayData.getClass()));
    if (originalFnForDisplayData instanceof HasDisplayData) {
      builder.include("fn", (HasDisplayData) originalFnForDisplayData);
    }
  }

  /**
   * Returns a new {@link MapWithFailures} transform that catches exceptions raised while mapping
   * elements, with the given type descriptor used for the failure collection but the exception
   * handler yet to be specified using {@link MapWithFailures#exceptionsVia(ProcessFunction)}.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public <NewFailureT> MapWithFailures<InputT, OutputT, NewFailureT> exceptionsInto(
      TypeDescriptor<NewFailureT> failureTypeDescriptor) {
    return new MapWithFailures<>(
        fn, originalFnForDisplayData, inputType, outputType, null, failureTypeDescriptor);
  }

  /**
   * Returns a new {@link MapWithFailures} transform that catches exceptions raised while mapping
   * elements, passing the raised exception instance and the input element being processed through
   * the given {@code exceptionHandler} and emitting the result to a failure collection.
   *
   * <p>This method takes advantage of the type information provided by {@link InferableFunction},
   * meaning that a call to {@link #exceptionsInto(TypeDescriptor)} may not be necessary.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Result<PCollection<String>, String>> result = words.apply(
   *     MapElements
   *         .into(TypeDescriptors.integers())
   *         .via((String word) -> 1 / word.length)  // Could throw ArithmeticException
   *         .exceptionsVia(new WithFailures.ExceptionAsMapHandler<String>() {}));
   * PCollection<Integer> output = result.output();
   * PCollection<String> failures = result.failures();
   * }</pre>
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public <FailureT> MapWithFailures<InputT, OutputT, FailureT> exceptionsVia(
      InferableFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
    return new MapWithFailures<>(
        fn,
        originalFnForDisplayData,
        inputType,
        outputType,
        exceptionHandler,
        exceptionHandler.getOutputTypeDescriptor());
  }

  /** A {@code PTransform} that adds exception handling to {@link MapElements}. */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public static class MapWithFailures<InputT, OutputT, FailureT>
      extends PTransform<PCollection<InputT>, WithFailures.Result<PCollection<OutputT>, FailureT>> {

    private final transient TypeDescriptor<InputT> inputType;
    private final transient TypeDescriptor<OutputT> outputType;
    private final transient @Nullable TypeDescriptor<FailureT> failureType;
    private final transient Object originalFnForDisplayData;
    private final Contextful<Fn<InputT, OutputT>> fn;
    private final @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler;

    MapWithFailures(
        Contextful<Fn<InputT, OutputT>> fn,
        Object originalFnForDisplayData,
        TypeDescriptor<InputT> inputType,
        TypeDescriptor<OutputT> outputType,
        @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler,
        @Nullable TypeDescriptor<FailureT> failureType) {
      this.fn = fn;
      this.originalFnForDisplayData = originalFnForDisplayData;
      this.inputType = inputType;
      this.outputType = outputType;
      this.exceptionHandler = exceptionHandler;
      this.failureType = failureType;
    }

    /**
     * Returns a {@code PTransform} that catches exceptions raised while mapping elements, passing
     * the raised exception instance and the input element being processed through the given {@code
     * exceptionHandler} and emitting the result to a failure collection.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * Result<PCollection<Integer>, String> result = words.apply(
     *     MapElements
     *         .into(TypeDescriptors.integers())
     *         .via((String word) -> 1 / word.length())  // Could throw ArithmeticException
     *         .exceptionsInto(TypeDescriptors.strings())
     *         .exceptionsVia((ExceptionElement<String> ee) -> ee.exception().getMessage()));
     * PCollection<Integer> output = result.output();
     * PCollection<String> failures = result.failures();
     * }</pre>
     */
    public MapWithFailures<InputT, OutputT, FailureT> exceptionsVia(
        ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
      return new MapWithFailures<>(
          fn, originalFnForDisplayData, inputType, outputType, exceptionHandler, failureType);
    }

    @Override
    public WithFailures.Result<PCollection<OutputT>, FailureT> expand(PCollection<InputT> input) {
      checkArgument(exceptionHandler != null, ".exceptionsVia() is required");
      MapFn doFn = new MapFn();
      PCollectionTuple tuple =
          input.apply(
              MapWithFailures.class.getSimpleName(),
              ParDo.of(doFn)
                  .withOutputTags(doFn.outputTag, TupleTagList.of(doFn.failureTag))
                  .withSideInputs(this.fn.getRequirements().getSideInputs()));
      return WithFailures.Result.of(tuple, doFn.outputTag, doFn.failureTag);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("class", originalFnForDisplayData.getClass()));
      if (originalFnForDisplayData instanceof HasDisplayData) {
        builder.include("fn", (HasDisplayData) originalFnForDisplayData);
      }
      builder.add(DisplayData.item("exceptionHandler.class", exceptionHandler.getClass()));
      if (exceptionHandler instanceof HasDisplayData) {
        builder.include("exceptionHandler", (HasDisplayData) exceptionHandler);
      }
    }

    /** A concrete TupleTag that allows coder inference based on failureType. */
    private class FailureTag extends TupleTag<FailureT> {
      @Override
      public TypeDescriptor<FailureT> getTypeDescriptor() {
        return failureType;
      }
    }

    /** A DoFn implementation that handles exceptions and outputs a secondary failure collection. */
    private class MapFn extends DoFn<InputT, OutputT> {

      final TupleTag<OutputT> outputTag = new TupleTag<OutputT>() {};
      final TupleTag<FailureT> failureTag = new FailureTag();

      @ProcessElement
      public void processElement(@Element InputT element, MultiOutputReceiver r, ProcessContext c)
          throws Exception {
        boolean exceptionWasThrown = false;
        OutputT result = null;
        try {
          result = fn.getClosure().apply(c.element(), Fn.Context.wrapProcessContext(c));
        } catch (Exception e) {
          exceptionWasThrown = true;
          ExceptionElement<InputT> exceptionElement = ExceptionElement.of(element, e);
          r.get(failureTag).output(exceptionHandler.apply(exceptionElement));
        }
        // We make sure our output occurs outside the try block, since runners may implement
        // fusion by having output() directly call the body of another DoFn, potentially catching
        // exceptions unrelated to this transform.
        if (!exceptionWasThrown) {
          r.get(outputTag).output(result);
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(MapWithFailures.this);
      }

      @Override
      public TypeDescriptor<InputT> getInputTypeDescriptor() {
        return inputType;
      }

      @Override
      public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
        checkState(
            outputType != null,
            "%s output type descriptor was null; "
                + "this probably means that getOutputTypeDescriptor() was called after "
                + "serialization/deserialization, but it is only available prior to "
                + "serialization, for constructing a pipeline and inferring coders",
            MapWithFailures.class.getSimpleName());
        return outputType;
      }
    }
  }
}
