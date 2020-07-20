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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.Nullable;
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
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link PTransform} for serializing objects to JSON {@link String Strings}. Transforms a {@code
 * PCollection<InputT>} into a {@link PCollection} of JSON {@link String Strings} representing
 * objects in the original {@link PCollection} using Jackson.
 */
public class AsJsons<InputT> extends PTransform<PCollection<InputT>, PCollection<String>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private final Class<? extends InputT> inputClass;
  private ObjectMapper customMapper;

  /**
   * Creates a {@link AsJsons} {@link PTransform} that will transform a {@code PCollection<InputT>}
   * into a {@link PCollection} of JSON {@link String Strings} representing those objects using a
   * Jackson {@link ObjectMapper}.
   */
  public static <InputT> AsJsons<InputT> of(Class<? extends InputT> inputClass) {
    return new AsJsons<>(inputClass);
  }

  private AsJsons(Class<? extends InputT> inputClass) {
    this.inputClass = inputClass;
  }

  /** Use custom Jackson {@link ObjectMapper} instead of the default one. */
  public AsJsons<InputT> withMapper(ObjectMapper mapper) {
    AsJsons<InputT> newTransform = new AsJsons<>(inputClass);
    newTransform.customMapper = mapper;
    return newTransform;
  }

  /**
   * Returns a new {@link AsJsonsWithFailures} transform that catches exceptions raised while
   * writing JSON elements, with the given type descriptor used for the failure collection but the
   * exception handler yet to be specified using {@link
   * AsJsonsWithFailures#exceptionsVia(ProcessFunction)}.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public <NewFailureT> AsJsonsWithFailures<NewFailureT> exceptionsInto(
      TypeDescriptor<NewFailureT> failureTypeDescriptor) {
    return new AsJsonsWithFailures<>(null, failureTypeDescriptor);
  }

  /**
   * Returns a new {@link AsJsonsWithFailures} transform that catches exceptions raised while
   * writing JSON elements, passing the raised exception instance and the input element being
   * processed through the given {@code exceptionHandler} and emitting the result to a failure
   * collection.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * WithFailures.Result<PCollection<String>, KV<MyPojo, Map<String, String>>> result =
   *     pojos.apply(
   *         AsJsons.of(MyPojo.class)
   *             .exceptionsVia(new WithFailures.ExceptionAsMapHandler<MyPojo>() {}));
   *
   * PCollection<String> output = result.output(); // valid json elements
   * PCollection<KV<MyPojo, Map<String, String>>> failures = result.failures();
   * }</pre>
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public <FailureT> AsJsonsWithFailures<FailureT> exceptionsVia(
      InferableFunction<WithFailures.ExceptionElement<InputT>, FailureT> exceptionHandler) {
    return new AsJsonsWithFailures<>(exceptionHandler, exceptionHandler.getOutputTypeDescriptor());
  }

  /**
   * Returns a new {@link AsJsonsWithFailures} transform that catches exceptions raised while
   * writing JSON elements, passing the raised exception instance and the input element being
   * processed through the default exception handler {@link DefaultExceptionAsMapHandler} and
   * emitting the result to a failure collection.
   *
   * <p>See {@link DefaultExceptionAsMapHandler} for more details about default handler behavior.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * WithFailures.Result<PCollection<String>, KV<MyPojo, Map<String, String>>> result =
   *     pojos.apply(
   *         AsJsons.of(MyPojo.class)
   *             .exceptionsVia());
   *
   * PCollection<String> output = result.output(); // valid json elements
   * PCollection<KV<MyPojo, Map<String, String>>> failures = result.failures();
   * }</pre>
   */
  @Experimental(Kind.WITH_EXCEPTIONS)
  public AsJsonsWithFailures<KV<InputT, Map<String, String>>> exceptionsVia() {
    DefaultExceptionAsMapHandler<InputT> exceptionHandler =
        new DefaultExceptionAsMapHandler<InputT>() {};
    return new AsJsonsWithFailures<>(exceptionHandler, exceptionHandler.getOutputTypeDescriptor());
  }

  private String writeValue(InputT input) throws JsonProcessingException {
    ObjectMapper mapper = Optional.ofNullable(customMapper).orElse(DEFAULT_MAPPER);
    return mapper.writeValueAsString(input);
  }

  @Override
  public PCollection<String> expand(PCollection<InputT> input) {
    return input.apply(
        MapElements.via(
            new SimpleFunction<InputT, String>() {
              @Override
              public String apply(InputT input) {
                try {
                  return writeValue(input);
                } catch (IOException e) {
                  throw new RuntimeException(
                      "Failed to serialize " + inputClass.getName() + " value: " + input, e);
                }
              }
            }));
  }

  /** A {@code PTransform} that adds exception handling to {@link AsJsons}. */
  public class AsJsonsWithFailures<FailureT>
      extends PTransform<PCollection<InputT>, WithFailures.Result<PCollection<String>, FailureT>> {

    private @Nullable InferableFunction<WithFailures.ExceptionElement<InputT>, FailureT>
        exceptionHandler;

    private final transient @Nullable TypeDescriptor<FailureT> failureType;

    AsJsonsWithFailures(
        InferableFunction<WithFailures.ExceptionElement<InputT>, FailureT> exceptionHandler,
        TypeDescriptor<FailureT> failureType) {
      this.exceptionHandler = exceptionHandler;
      this.failureType = failureType;
    }

    /**
     * Returns a new {@link AsJsonsWithFailures} transform that catches exceptions raised while
     * writing JSON elements, passing the raised exception instance and the input element being
     * processed through the given {@code exceptionHandler} and emitting the result to a failure
     * collection. It is supposed to be used along with {@link
     * AsJsons#exceptionsInto(TypeDescriptor)} and get lambda function as exception handler.
     *
     * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
     * WithFailures.Result}.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * WithFailures.Result<PCollection<String>, KV<MyPojo, Map<String, String>>> result =
     *     pojos.apply(
     *         AsJsons.of(MyPojo.class)
     *             .exceptionsInto(
     *                 TypeDescriptors.kvs(
     *                     TypeDescriptor.of(MyPojo.class), TypeDescriptors.strings()))
     *             .exceptionsVia(
     *                 f -> KV.of(f.element(), f.exception().getClass().getCanonicalName())));
     *
     * PCollection<String> output = result.output(); // valid json elements
     * PCollection<KV<MyPojo, Map<String, String>>> failures = result.failures();
     * }</pre>
     */
    public AsJsonsWithFailures<FailureT> exceptionsVia(
        ProcessFunction<WithFailures.ExceptionElement<InputT>, FailureT> exceptionHandler) {
      return new AsJsonsWithFailures<>(
          new InferableFunction<WithFailures.ExceptionElement<InputT>, FailureT>(
              exceptionHandler) {},
          failureType);
    }

    @Override
    public WithFailures.Result<PCollection<String>, FailureT> expand(PCollection<InputT> input) {
      return input.apply(
          MapElements.into(TypeDescriptors.strings())
              .via(
                  Contextful.fn(
                      (Contextful.Fn<InputT, String>) (input1, c) -> writeValue(input1),
                      Requirements.empty()))
              .exceptionsInto(failureType)
              .exceptionsVia(exceptionHandler));
    }
  }

  /**
   * A default handler that extracts information from an exception to a {@code Map<String, String>}
   * and returns a {@link KV} where the key is the input element that failed processing, and the
   * value is the map of exception attributes. It handles only {@code JsonProcessingException},
   * other type of exceptions will be rethrown as {@code RuntimeException}.
   *
   * <p>The keys populated in the map are "className", "message", and "stackTrace" of the exception.
   */
  private static class DefaultExceptionAsMapHandler<InputT>
      extends SimpleFunction<
          WithFailures.ExceptionElement<InputT>, KV<InputT, Map<String, String>>> {
    @Override
    public KV<InputT, Map<String, String>> apply(WithFailures.ExceptionElement<InputT> f)
        throws RuntimeException {
      if (!(f.exception() instanceof JsonProcessingException)) {
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
