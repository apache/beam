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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

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
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class MapElements<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {

  private final transient @Nullable TypeDescriptor<InputT> inputType;
  private final transient TypeDescriptor<OutputT> outputType;

  private final @Nullable Object fn;

  private MapElements(
      @Nullable Object fn,
      @Nullable TypeDescriptor<InputT> inputType,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
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
    return new MapElements<>(fn, fn.getInputTypeDescriptor(), fn.getOutputTypeDescriptor());
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
    return new MapElements<>(null, null, outputType);
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
    return new MapElements<>(fn, TypeDescriptors.inputOf(fn), outputType);
  }

  /** Binary compatibility adapter for {@link #via(ProcessFunction)}. */
  public <NewInputT> MapElements<NewInputT, OutputT> via(
      SerializableFunction<NewInputT, OutputT> fn) {
    return via((ProcessFunction<NewInputT, OutputT>) fn);
  }

  /** Like {@link #via(ProcessFunction)}, but supports access to context, such as side inputs. */
  public <NewInputT> MapElements<NewInputT, OutputT> via(Contextful<Fn<NewInputT, OutputT>> fn) {
    return new MapElements<>(fn, TypeDescriptors.inputOf(fn.getClosure()), outputType);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkNotNull(fn, "Must specify a function on MapElements using .via()");
    if (fn instanceof Contextful) {
      return input.apply(
          "Map",
          ParDo.of(
                  new MapDoFn() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                      c.output(
                          ((Contextful<Fn<InputT, OutputT>>) fn)
                              .getClosure()
                              .apply(c.element(), Fn.Context.wrapProcessContext(c)));
                    }
                  })
              .withSideInputs(
                  ((Contextful<Fn<InputT, OutputT>>) fn).getRequirements().getSideInputs()));
    } else if (fn instanceof ProcessFunction) {
      return input.apply(
          "Map",
          ParDo.of(
              new MapDoFn() {
                @ProcessElement
                public void processElement(
                    @Element InputT element, OutputReceiver<OutputT> receiver) throws Exception {
                  receiver.output(((ProcessFunction<InputT, OutputT>) fn).apply(element));
                }
              }));
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown type of fn class %s", fn.getClass()));
    }
  }

  /** A DoFn implementation that handles a trivial map call. */
  private abstract class MapDoFn extends DoFn<InputT, OutputT> {
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
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    Object fnForDisplayData;
    if (fn instanceof Contextful) {
      fnForDisplayData = ((Contextful<?>) fn).getClosure();
    } else {
      fnForDisplayData = fn;
    }
    builder.add(DisplayData.item("class", fnForDisplayData.getClass()));
    if (fnForDisplayData instanceof HasDisplayData) {
      builder.include("fn", (HasDisplayData) fnForDisplayData);
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
  public <NewFailureT> MapWithFailures<InputT, OutputT, NewFailureT> exceptionsInto(
      TypeDescriptor<NewFailureT> failureTypeDescriptor) {
    return new MapWithFailures<>(fn, inputType, outputType, null, failureTypeDescriptor);
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
  public <FailureT> MapWithFailures<InputT, OutputT, FailureT> exceptionsVia(
      InferableFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
    return new MapWithFailures<>(
        fn, inputType, outputType, exceptionHandler, exceptionHandler.getOutputTypeDescriptor());
  }

  /** A {@code PTransform} that adds exception handling to {@link MapElements}. */
  public static class MapWithFailures<InputT, OutputT, FailureT>
      extends PTransform<PCollection<InputT>, WithFailures.Result<PCollection<OutputT>, FailureT>> {

    private final transient TypeDescriptor<InputT> inputType;
    private final transient TypeDescriptor<OutputT> outputType;
    private final transient @Nullable TypeDescriptor<FailureT> failureType;
    private final Object fn;
    private final @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler;

    MapWithFailures(
        Object fn,
        TypeDescriptor<InputT> inputType,
        TypeDescriptor<OutputT> outputType,
        @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler,
        @Nullable TypeDescriptor<FailureT> failureType) {
      this.fn = fn;
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
      return new MapWithFailures<>(fn, inputType, outputType, exceptionHandler, failureType);
    }

    @Override
    public WithFailures.Result<PCollection<OutputT>, FailureT> expand(PCollection<InputT> input) {
      checkArgument(exceptionHandler != null, ".exceptionsVia() is required");
      MapWithFailuresDoFn doFn;
      PCollectionTuple tuple;
      if (fn instanceof Contextful) {
        doFn =
            new MapWithFailuresDoFn() {
              @ProcessElement
              public void processElement(@Element InputT element, ProcessContext c)
                  throws Exception {
                boolean exceptionWasThrown = false;
                OutputT result = null;
                try {
                  result =
                      ((Contextful<Fn<InputT, OutputT>>) fn)
                          .getClosure()
                          .apply(element, Fn.Context.wrapProcessContext(c));
                } catch (Exception e) {
                  exceptionWasThrown = true;
                  ExceptionElement<InputT> exceptionElement = ExceptionElement.of(element, e);
                  c.output(failureTag, exceptionHandler.apply(exceptionElement));
                }
                // We make sure our output occurs outside the try block, since runners may implement
                // fusion by having output() directly call the body of another DoFn, potentially
                // catching
                // exceptions unrelated to this transform.
                if (!exceptionWasThrown) {
                  c.output(result);
                }
              }
            };
        tuple =
            input.apply(
                MapWithFailures.class.getSimpleName(),
                ParDo.of(doFn)
                    .withOutputTags(doFn.outputTag, TupleTagList.of(doFn.failureTag))
                    .withSideInputs(
                        ((Contextful<Fn<InputT, OutputT>>) fn).getRequirements().getSideInputs()));

      } else if (fn instanceof ProcessFunction) {
        ProcessFunction<InputT, OutputT> closure = (ProcessFunction<InputT, OutputT>) fn;
        doFn =
            new MapWithFailuresDoFn() {
              @ProcessElement
              public void processElement(@Element InputT element, ProcessContext c)
                  throws Exception {
                boolean exceptionWasThrown = false;
                OutputT result;
                try {
                  result = closure.apply(element);
                } catch (Exception e) {
                  result = null;
                  exceptionWasThrown = true;
                  ExceptionElement<InputT> exceptionElement = ExceptionElement.of(element, e);
                  c.output(failureTag, exceptionHandler.apply(exceptionElement));
                }
                // We make sure our output occurs outside the try block, since runners may implement
                // fusion by having output() directly call the body of another DoFn, potentially
                // catching
                // exceptions unrelated to this transform.
                if (!exceptionWasThrown) {
                  c.output(result);
                }
              }
            };
        tuple =
            input.apply(
                MapWithFailures.class.getSimpleName(),
                ParDo.of(doFn).withOutputTags(doFn.outputTag, TupleTagList.of(doFn.failureTag)));
      } else {
        throw new IllegalArgumentException(
            String.format("Unknown type of fn class %s", fn.getClass()));
      }
      return WithFailures.Result.of(tuple, doFn.outputTag, doFn.failureTag);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Object fnForDisplayData;
      if (fn instanceof Contextful) {
        fnForDisplayData = ((Contextful<?>) fn).getClosure();
      } else {
        fnForDisplayData = fn;
      }
      builder.add(DisplayData.item("class", fnForDisplayData.getClass()));
      if (fnForDisplayData instanceof HasDisplayData) {
        builder.include("fn", (HasDisplayData) fnForDisplayData);
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
    private abstract class MapWithFailuresDoFn extends DoFn<InputT, OutputT> {
      final TupleTag<OutputT> outputTag = new TupleTag<OutputT>() {};
      final TupleTag<FailureT> failureTag = new FailureTag();

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
