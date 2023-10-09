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

/**
 * {@code PTransform}s for mapping a simple function that returns iterables over the elements of a
 * {@link PCollection} and merging the results.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FlatMapElements<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  private final transient @Nullable TypeDescriptor<InputT> inputType;
  private final transient @Nullable TypeDescriptor<OutputT> outputType;
  private final @Nullable Object fn;

  private FlatMapElements(
      @Nullable Object fn,
      @Nullable TypeDescriptor<InputT> inputType,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  /**
   * For a {@code InferableFunction<InputT, ? extends Iterable<OutputT>>} {@code fn}, return a
   * {@link PTransform} that applies {@code fn} to every element of the input {@code
   * PCollection<InputT>} and outputs all of the elements to the output {@code
   * PCollection<OutputT>}.
   *
   * <p>{@link InferableFunction} has the advantage of providing type descriptor information, but it
   * is generally more convenient to specify output type via {@link #into(TypeDescriptor)}, and
   * provide the mapping as a lambda expression to {@link #via(ProcessFunction)}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<String> lines = ...;
   * PCollection<String> words = lines.apply(FlatMapElements.via(
   *     new InferableFunction<String, List<String>>() {
   *       public List<String> apply(String line) throws Exception {
   *         return Arrays.asList(line.split(" "));
   *       }
   *     });
   * }</pre>
   */
  public static <InputT, OutputT> FlatMapElements<InputT, OutputT> via(
      InferableFunction<? super InputT, ? extends Iterable<OutputT>> fn) {
    TypeDescriptor<OutputT> outputType =
        TypeDescriptors.extractFromTypeParameters(
            (TypeDescriptor<Iterable<OutputT>>) fn.getOutputTypeDescriptor(),
            Iterable.class,
            new TypeDescriptors.TypeVariableExtractor<Iterable<OutputT>, OutputT>() {});
    TypeDescriptor<InputT> inputType = (TypeDescriptor<InputT>) fn.getInputTypeDescriptor();
    return new FlatMapElements<>(fn, inputType, outputType);
  }

  /** Binary compatibility adapter for {@link #via(ProcessFunction)}. */
  public static <InputT, OutputT> FlatMapElements<InputT, OutputT> via(
      SimpleFunction<? super InputT, ? extends Iterable<OutputT>> fn) {
    return via((InferableFunction<? super InputT, ? extends Iterable<OutputT>>) fn);
  }

  /**
   * Returns a new {@link FlatMapElements} transform with the given type descriptor for the output
   * type, but the mapping function yet to be specified using {@link #via(ProcessFunction)}.
   */
  public static <OutputT> FlatMapElements<?, OutputT> into(
      final TypeDescriptor<OutputT> outputType) {
    return new FlatMapElements<>(null, null, outputType);
  }

  /**
   * For a {@code ProcessFunction<InputT, ? extends Iterable<OutputT>>} {@code fn}, returns a {@link
   * PTransform} that applies {@code fn} to every element of the input {@code PCollection<InputT>}
   * and outputs all of the elements to the output {@code PCollection<OutputT>}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<String> words = lines.apply(
   *     FlatMapElements.into(TypeDescriptors.strings())
   *                    .via((String line) -> Arrays.asList(line.split(" ")))
   * }</pre>
   */
  public <NewInputT> FlatMapElements<NewInputT, OutputT> via(
      ProcessFunction<NewInputT, ? extends Iterable<OutputT>> fn) {
    return new FlatMapElements<>(fn, TypeDescriptors.inputOf(fn), outputType);
  }

  /** Binary compatibility adapter for {@link #via(ProcessFunction)}. */
  public <NewInputT> FlatMapElements<NewInputT, OutputT> via(
      SerializableFunction<NewInputT, ? extends Iterable<OutputT>> fn) {
    return via((ProcessFunction<NewInputT, ? extends Iterable<OutputT>>) fn);
  }

  /** Like {@link #via(ProcessFunction)}, but allows access to additional context. */
  public <NewInputT> FlatMapElements<NewInputT, OutputT> via(
      Contextful<Fn<NewInputT, Iterable<OutputT>>> fn) {
    return new FlatMapElements<>(fn, TypeDescriptors.inputOf(fn.getClosure()), outputType);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkArgument(fn != null, ".via() is required");
    if (fn instanceof Contextful) {
      return input.apply(
          "FlatMap",
          ParDo.of(
                  new FlatMapDoFn() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                      Iterable<OutputT> res =
                          ((Contextful<Fn<InputT, Iterable<OutputT>>>) fn)
                              .getClosure()
                              .apply(c.element(), Fn.Context.wrapProcessContext(c));
                      for (OutputT output : res) {
                        c.output(output);
                      }
                    }
                  })
              .withSideInputs(
                  ((Contextful<Fn<InputT, Iterable<OutputT>>>) fn)
                      .getRequirements()
                      .getSideInputs()));
    } else if (fn instanceof ProcessFunction) {
      return input.apply(
          "FlatMap",
          ParDo.of(
              new FlatMapDoFn() {
                @ProcessElement
                public void processElement(
                    @Element InputT element, OutputReceiver<OutputT> receiver) throws Exception {
                  Iterable<OutputT> res =
                      ((ProcessFunction<InputT, Iterable<OutputT>>) fn).apply(element);
                  for (OutputT output : res) {
                    receiver.output(output);
                  }
                }
              }));
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown type of fn class %s", fn.getClass()));
    }
  }

  private abstract class FlatMapDoFn extends DoFn<InputT, OutputT> {

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
          FlatMapElements.class.getSimpleName());
      return outputType;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(FlatMapElements.this);
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    Object fnForDisplayData;
    if (fn instanceof Contextful) {
      fnForDisplayData = ((Contextful<Fn<InputT, OutputT>>) fn).getClosure();
    } else {
      fnForDisplayData = fn;
    }
    builder.add(DisplayData.item("class", fnForDisplayData.getClass()));
    if (fnForDisplayData instanceof HasDisplayData) {
      builder.include("fn", (HasDisplayData) fnForDisplayData);
    }
  }

  /**
   * Returns a new {@link FlatMapWithFailures} transform that catches exceptions raised while
   * mapping elements, with the given type descriptor used for the failure collection but the
   * exception handler yet to be specified using {@link
   * FlatMapWithFailures#exceptionsVia(ProcessFunction)}.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   */
  public <NewFailureT> FlatMapWithFailures<InputT, OutputT, NewFailureT> exceptionsInto(
      TypeDescriptor<NewFailureT> failureTypeDescriptor) {
    return new FlatMapWithFailures<>(fn, inputType, outputType, null, failureTypeDescriptor);
  }

  /**
   * Returns a new {@link FlatMapWithFailures} transform that catches exceptions raised while
   * mapping elements, passing the raised exception instance and the input element being processed
   * through the given {@code exceptionHandler} and emitting the result to a failure collection.
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
   *     FlatMapElements
   *         .into(TypeDescriptors.strings())
   *         // Could throw ArrayIndexOutOfBoundsException
   *         .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
   *         .exceptionsVia(new WithFailures.ExceptionAsMapHandler<String>() {}));
   * PCollection<String> output = result.output();
   * PCollection<String> failures = result.failures();
   * }</pre>
   */
  public <FailureT> FlatMapWithFailures<InputT, OutputT, FailureT> exceptionsVia(
      InferableFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
    return new FlatMapWithFailures<>(
        fn, inputType, outputType, exceptionHandler, exceptionHandler.getOutputTypeDescriptor());
  }

  /** A {@code PTransform} that adds exception handling to {@link FlatMapElements}. */
  public static class FlatMapWithFailures<InputT, OutputT, FailureT>
      extends PTransform<PCollection<InputT>, WithFailures.Result<PCollection<OutputT>, FailureT>> {

    private final transient TypeDescriptor<InputT> inputType;
    private final transient TypeDescriptor<OutputT> outputType;
    private final transient @Nullable TypeDescriptor<FailureT> failureType;
    private final @Nullable Object fn;
    private final @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler;

    FlatMapWithFailures(
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
     * Returns a new {@link FlatMapWithFailures} transform that catches exceptions raised while
     * mapping elements, passing the raised exception instance and the input element being processed
     * through the given {@code exceptionHandler} and emitting the result to a failure collection.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * Result<PCollection<String>, String>> result = words.apply(
     *     FlatMapElements
     *         .into(TypeDescriptors.strings())
     *         // Could throw ArrayIndexOutOfBoundsException
     *         .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
     *         .exceptionsInto(TypeDescriptors.strings())
     *         .exceptionsVia((ExceptionElement<String> ee) -> ee.exception().getMessage()));
     * PCollection<String> output = result.output();
     * PCollection<String> failures = result.failures();
     * }</pre>
     */
    public FlatMapWithFailures<InputT, OutputT, FailureT> exceptionsVia(
        ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
      return new FlatMapWithFailures<>(fn, inputType, outputType, exceptionHandler, failureType);
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
                Iterable<OutputT> res = null;
                try {
                  res =
                      ((Contextful<Fn<InputT, Iterable<OutputT>>>) fn)
                          .getClosure()
                          .apply(element, Fn.Context.wrapProcessContext(c));
                } catch (Exception e) {
                  exceptionWasThrown = true;
                  ExceptionElement<InputT> exceptionElement = ExceptionElement.of(element, e);
                  c.output(failureTag, exceptionHandler.apply(exceptionElement));
                }
                // We make sure our outputs occur outside the try block, since runners may implement
                // fusion by having output() directly call the body of another DoFn, potentially
                // catching
                // exceptions unrelated to this transform.
                if (!exceptionWasThrown) {
                  for (OutputT output : res) {
                    c.output(output);
                  }
                }
              }
            };
        tuple =
            input.apply(
                FlatMapWithFailures.class.getSimpleName(),
                ParDo.of(doFn)
                    .withOutputTags(doFn.outputTag, TupleTagList.of(doFn.failureTag))
                    .withSideInputs(
                        ((Contextful<Fn<InputT, Iterable<OutputT>>>) fn)
                            .getRequirements()
                            .getSideInputs()));
      } else if (fn instanceof ProcessFunction) {
        doFn =
            new MapWithFailuresDoFn() {
              @ProcessElement
              public void processElement(@Element InputT element, ProcessContext c)
                  throws Exception {
                boolean exceptionWasThrown = false;
                Iterable<OutputT> res = null;
                try {
                  res = ((ProcessFunction<InputT, Iterable<OutputT>>) fn).apply(element);
                } catch (Exception e) {
                  exceptionWasThrown = true;
                  ExceptionElement<InputT> exceptionElement = ExceptionElement.of(element, e);
                  c.output(failureTag, exceptionHandler.apply(exceptionElement));
                }
                // We make sure our outputs occur outside the try block, since runners may implement
                // fusion by having output() directly call the body of another DoFn, potentially
                // catching
                // exceptions unrelated to this transform.
                if (!exceptionWasThrown) {
                  for (OutputT output : res) {
                    c.output(output);
                  }
                }
              }
            };
        tuple =
            input.apply(
                FlatMapWithFailures.class.getSimpleName(),
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
        builder.delegate(FlatMapWithFailures.this);
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
            FlatMapWithFailures.class.getSimpleName());
        return outputType;
      }
    }
  }
}
