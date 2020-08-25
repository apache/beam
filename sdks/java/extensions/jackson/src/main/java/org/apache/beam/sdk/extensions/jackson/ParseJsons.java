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
package org.apache.beam.sdk.extensions.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link PTransform} for parsing JSON {@link String Strings}. Parse {@link PCollection} of {@link
 * String Strings} in JSON format into a {@link PCollection} of objects represented by those JSON
 * {@link String Strings} using Jackson.
 */
public class ParseJsons<OutputT> extends PTransform<PCollection<String>, PCollection<OutputT>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private final Class<? extends OutputT> outputClass;
  private ObjectMapper customMapper;

  /**
   * Creates a {@link ParseJsons} {@link PTransform} that will parse JSON {@link String Strings}
   * into a {@code PCollection<OutputT>} using a Jackson {@link ObjectMapper}.
   */
  public static <OutputT> ParseJsons<OutputT> of(Class<? extends OutputT> outputClass) {
    return new ParseJsons<>(outputClass);
  }

  private ParseJsons(Class<? extends OutputT> outputClass) {
    this.outputClass = outputClass;
  }

  /** Use custom Jackson {@link ObjectMapper} instead of the default one. */
  public ParseJsons<OutputT> withMapper(ObjectMapper mapper) {
    ParseJsons<OutputT> newTransform = new ParseJsons<>(outputClass);
    newTransform.customMapper = mapper;
    return newTransform;
  }

  /**
   * Returns a new {@link ParseJsonsWithFailures} transform that catches exceptions raised while
   * parsing elements, with the given type descriptor used for the failure collection but the
   * exception handler yet to be specified using {@link
   * ParseJsonsWithFailures#exceptionsVia(ProcessFunction)}.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public <NewFailureT> ParseJsonsWithFailures<NewFailureT> exceptionsInto(
      TypeDescriptor<NewFailureT> failureTypeDescriptor) {
    return new ParseJsonsWithFailures<>(null, failureTypeDescriptor);
  }

  /**
   * Returns a new {@link ParseJsonsWithFailures} transform that catches exceptions raised while
   * parsing elements, passing the raised exception instance and the input element being processed
   * through the given {@code exceptionHandler} and emitting the result to a failure collection.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * WithFailures.Result<PCollection<MyPojo>, KV<String, Map<String, String>>> result =
   *     json.apply(
   *         ParseJsons.of(MyPojo.class)
   *             .exceptionsVia(new WithFailures.ExceptionAsMapHandler<String>() {}));
   *
   * PCollection<MyPojo> output = result.output(); // valid POJOs
   * PCollection<KV<String, Map<String, String>>> failures = result.failures();
   * }</pre>
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public <FailureT> ParseJsonsWithFailures<FailureT> exceptionsVia(
      InferableFunction<WithFailures.ExceptionElement<String>, FailureT> exceptionHandler) {
    return new ParseJsonsWithFailures<>(
        exceptionHandler, exceptionHandler.getOutputTypeDescriptor());
  }

  /**
   * Returns a new {@link ParseJsonsWithFailures} transform that catches exceptions raised while
   * parsing elements, passing the raised exception instance and the input element being processed
   * through the default exception handler {@link DefaultExceptionAsMapHandler} and emitting the
   * result to a failure collection.
   *
   * <p>See {@link DefaultExceptionAsMapHandler} for more details about default handler behavior.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * WithFailures.Result<PCollection<MyPojo>, KV<String, Map<String, String>>> result =
   *     json.apply(
   *         ParseJsons.of(MyPojo.class)
   *             .exceptionsVia());
   *
   * PCollection<MyPojo> output = result.output(); // valid POJOs
   * PCollection<KV<String, Map<String, String>>> failures = result.failures();
   * }</pre>
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public ParseJsonsWithFailures<KV<String, Map<String, String>>> exceptionsVia() {
    DefaultExceptionAsMapHandler<String> exceptionHandler =
        new DefaultExceptionAsMapHandler<String>() {};
    return new ParseJsonsWithFailures<>(
        exceptionHandler, exceptionHandler.getOutputTypeDescriptor());
  }

  private OutputT readValue(String input) throws IOException {
    ObjectMapper mapper = Optional.ofNullable(customMapper).orElse(DEFAULT_MAPPER);
    return mapper.readValue(input, outputClass);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<String> input) {
    return input.apply(
        MapElements.via(
            new SimpleFunction<String, OutputT>() {
              @Override
              public OutputT apply(String input) {
                try {
                  return readValue(input);
                } catch (IOException e) {
                  throw new RuntimeException(
                      "Failed to parse a " + outputClass.getName() + " from JSON value: " + input,
                      e);
                }
              }
            }));
  }

  /** A {@code PTransform} that adds exception handling to {@link ParseJsons}. */
  public class ParseJsonsWithFailures<FailureT>
      extends PTransform<PCollection<String>, WithFailures.Result<PCollection<OutputT>, FailureT>> {

    private @Nullable InferableFunction<WithFailures.ExceptionElement<String>, FailureT>
        exceptionHandler;

    private final transient @Nullable TypeDescriptor<FailureT> failureType;

    ParseJsonsWithFailures(
        InferableFunction<WithFailures.ExceptionElement<String>, FailureT> exceptionHandler,
        TypeDescriptor<FailureT> failureType) {
      this.exceptionHandler = exceptionHandler;
      this.failureType = failureType;
    }

    /**
     * Returns a new {@link ParseJsonsWithFailures} transform that catches exceptions raised while
     * parsing elements, passing the raised exception instance and the input element being processed
     * through the given {@code exceptionHandler} and emitting the result to a failure collection.
     * It is supposed to be used along with {@link ParseJsons#exceptionsInto(TypeDescriptor)} and
     * get lambda function as exception handler.
     *
     * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
     * WithFailures.Result}.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * WithFailures.Result<PCollection<MyPojo>, KV<String, Map<String, String>>> result =
     *     json.apply(
     *         ParseJsons.of(MyPojo.class)
     *             .exceptionsInto(
     *                 TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     *             .exceptionsVia(
     *                 f -> KV.of(f.element(), f.exception().getClass().getCanonicalName())));
     *
     * PCollection<MyPojo> output = result.output(); // valid POJOs
     * PCollection<KV<String, Map<String, String>>> failures = result.failures();
     * }</pre>
     */
    public ParseJsonsWithFailures<FailureT> exceptionsVia(
        ProcessFunction<WithFailures.ExceptionElement<String>, FailureT> exceptionHandler) {
      return new ParseJsonsWithFailures<>(
          new InferableFunction<WithFailures.ExceptionElement<String>, FailureT>(
              exceptionHandler) {},
          failureType);
    }

    @Override
    public WithFailures.Result<PCollection<OutputT>, FailureT> expand(PCollection<String> input) {
      return input.apply(
          MapElements.into(new TypeDescriptor<OutputT>() {})
              .via(
                  Contextful.fn(
                      (Contextful.Fn<String, OutputT>) (input1, c) -> readValue(input1),
                      Requirements.empty()))
              .exceptionsInto(failureType)
              .exceptionsVia(exceptionHandler));
    }
  }

  /**
   * A default handler that extracts information from an exception to a {@code Map<String, String>}
   * and returns a {@link KV} where the key is the input element that failed processing, and the
   * value is the map of exception attributes. It handles only {@code IOException}, other type of
   * exceptions will be rethrown as {@code RuntimeException}.
   *
   * <p>The keys populated in the map are "className", "message", and "stackTrace" of the exception.
   */
  private static class DefaultExceptionAsMapHandler<OutputT>
      extends SimpleFunction<
          WithFailures.ExceptionElement<OutputT>, KV<OutputT, Map<String, String>>> {
    @Override
    public KV<OutputT, Map<String, String>> apply(WithFailures.ExceptionElement<OutputT> f)
        throws RuntimeException {
      if (!(f.exception() instanceof IOException)) {
        throw new RuntimeException(f.exception());
      }
      return KV.of(
          f.element(),
          ImmutableMap.of(
              "className", f.exception().getClass().getName(),
              "message", f.exception().getMessage(),
              "stackTrace", Arrays.toString(f.exception().getStackTrace())));
    }
  }
}
