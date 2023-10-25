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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.beam.io.requestresponse.Call.Result;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Call}. */
@RunWith(JUnit4.class)
public class CallTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void givenCallerNotSerializable_throwsError() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Call.of(new UnSerializableCaller(), ResponseCoder.of()));
  }

  @Test
  public void givenSetupTeardownNotSerializable_throwsError() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Call.ofCallerAndSetupTeardown(
                new UnSerializableCallerWithSetupTeardown(), ResponseCoder.of()));
  }

  @Test
  public void givenCallerThrowsUserCodeExecutionException_emitsIntoFailurePCollection() {
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("a")))
            .apply(Call.of(new CallerThrowsUserCodeExecutionException(), ResponseCoder.of()));

    PCollection<ApiIOError> failures = result.getFailures();
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeExecutionException.class))
        .isEqualTo(1L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeQuotaException.class)).isEqualTo(0L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeTimeoutException.class))
        .isEqualTo(0L);

    pipeline.run();
  }

  @Test
  public void givenCallerThrowsQuotaException_emitsIntoFailurePCollection() {
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("a")))
            .apply(Call.of(new CallerInvokesQuotaException(), ResponseCoder.of()));

    PCollection<ApiIOError> failures = result.getFailures();
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeExecutionException.class))
        .isEqualTo(0L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeQuotaException.class)).isEqualTo(1L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeTimeoutException.class))
        .isEqualTo(0L);

    pipeline.run();
  }

  @Test
  public void givenCallerTimeout_emitsFailurePCollection() {
    Duration timeout = Duration.standardSeconds(1L);
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("a")))
            .apply(
                Call.of(new CallerExceedsTimeout(timeout), ResponseCoder.of())
                    .withTimeout(timeout));

    PCollection<ApiIOError> failures = result.getFailures();
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeExecutionException.class))
        .isEqualTo(0L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeQuotaException.class)).isEqualTo(0L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeTimeoutException.class))
        .isEqualTo(1L);

    pipeline.run();
  }

  public void givenCallerReturnsNull_emitsIntoOutput() {
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("")))
            .apply(Call.of(new CallerReturnsNullResponse(), ResponseCoder.of()));

    PAssert.thatSingleton(result.getFailures().apply("count/failures", Count.globally()))
        .isEqualTo(0L);
    PAssert.thatSingleton(result.getResponses().apply("count/successes", Count.globally()))
        .isEqualTo(1L);

    pipeline.run();
  }

  @Test
  public void givenCallerThrowsTimeoutException_emitsFailurePCollection() {
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("a")))
            .apply(Call.of(new CallerThrowsTimeout(), ResponseCoder.of()));

    PCollection<ApiIOError> failures = result.getFailures();
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeExecutionException.class))
        .isEqualTo(1L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeQuotaException.class)).isEqualTo(0L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeTimeoutException.class))
        .isEqualTo(1L);

    pipeline.run();
  }

  @Test
  public void givenSetupThrowsUserCodeExecutionException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withSetupTeardown(new SetupThrowsUserCodeExecutionException()));

    assertPipelineThrows(UserCodeExecutionException.class, pipeline);
  }

  @Test
  public void givenSetupThrowsQuotaException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withSetupTeardown(new SetupThrowsUserCodeQuotaException()));

    assertPipelineThrows(UserCodeQuotaException.class, pipeline);
  }

  @Test
  public void givenSetupTimeout_throwsError() {
    Duration timeout = Duration.standardSeconds(1L);

    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withSetupTeardown(new SetupExceedsTimeout(timeout))
                .withTimeout(timeout));

    assertPipelineThrows(UserCodeTimeoutException.class, pipeline);
  }

  @Test
  public void givenSetupThrowsTimeoutException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withSetupTeardown(new SetupThrowsUserCodeTimeoutException()));

    assertPipelineThrows(UserCodeTimeoutException.class, pipeline);
  }

  @Test
  public void givenTeardownThrowsUserCodeExecutionException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withSetupTeardown(new TeardownThrowsUserCodeExecutionException()));

    // Exceptions thrown during teardown do not populate with the cause
    assertThrows(IllegalStateException.class, () -> pipeline.run());
  }

  @Test
  public void givenTeardownThrowsQuotaException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withSetupTeardown(new TeardownThrowsUserCodeQuotaException()));

    // Exceptions thrown during teardown do not populate with the cause
    assertThrows(IllegalStateException.class, () -> pipeline.run());
  }

  @Test
  public void givenTeardownTimeout_throwsError() {
    Duration timeout = Duration.standardSeconds(1L);
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withTimeout(timeout)
                .withSetupTeardown(new TeardownExceedsTimeout(timeout)));

    // Exceptions thrown during teardown do not populate with the cause
    assertThrows(IllegalStateException.class, () -> pipeline.run());
  }

  @Test
  public void givenTeardownThrowsTimeoutException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), ResponseCoder.of())
                .withSetupTeardown(new TeardownThrowsUserCodeTimeoutException()));

    // Exceptions thrown during teardown do not populate with the cause
    assertThrows(IllegalStateException.class, () -> pipeline.run());
  }

  private static class ValidCaller implements Caller<Request, Response> {

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      return new Response(request.id);
    }
  }

  private static class UnSerializableCaller implements Caller<Request, Response> {

    @SuppressWarnings({"unused"})
    private final UnSerializable nestedThing = new UnSerializable();

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      return new Response(request.id);
    }
  }

  private static class UnSerializableCallerWithSetupTeardown extends UnSerializableCaller
      implements SetupTeardown {

    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class UnSerializable {}

  @DefaultCoder(RequestCoder.class)
  private static class Request implements Serializable {

    final String id;

    Request(String id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Request request = (Request) o;
      return Objects.equal(id, request.id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }
  }

  @DefaultCoder(ResponseCoder.class)
  private static class Response implements Serializable {
    final String id;

    Response(String id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Response response = (Response) o;
      return Objects.equal(id, response.id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }
  }

  // RequestCoder needs to be public otherwise pipeline defaults to SerializableCoder which throws
  // an error for being non-deterministic.
  public static class RequestCoder extends AtomicCoder<@NonNull Request> {

    public static CoderProvider getCoderProvider() {
      return CoderProviders.forCoder(TypeDescriptor.of(Request.class), new RequestCoder());
    }

    @Override
    public void encode(@NonNull Request value, @NonNull OutputStream outStream)
        throws @NonNull CoderException, @NonNull IOException {
      outStream.write(value.id.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public @NonNull Request decode(@NonNull InputStream inStream)
        throws @NonNull CoderException, @NonNull IOException {
      byte[] bytes = ByteStreams.toByteArray(inStream);
      return new Request(new String(bytes, StandardCharsets.UTF_8));
    }
  }

  private static class ResponseCoder extends AtomicCoder<@NonNull Response> {

    static ResponseCoder of() {
      return new ResponseCoder();
    }

    @Override
    public void encode(@NonNull Response value, @NonNull OutputStream outStream)
        throws @NonNull CoderException, @NonNull IOException {
      outStream.write(value.id.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public @NonNull Response decode(@NonNull InputStream inStream)
        throws @NonNull CoderException, @NonNull IOException {
      byte[] bytes = ByteStreams.toByteArray(inStream);
      return new Response(new String(bytes, StandardCharsets.UTF_8));
    }
  }

  private static class CallerExceedsTimeout implements Caller<Request, Response> {
    private final Duration timeout;

    CallerExceedsTimeout(Duration timeout) {
      this.timeout = timeout.plus(Duration.standardSeconds(1L));
    }

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      sleep(timeout);
      return new Response(request.id);
    }
  }

  private static class CallerReturnsNullResponse implements Caller<Request, Response> {

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      return null;
    }
  }

  private static class CallerThrowsUserCodeExecutionException implements Caller<Request, Response> {

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      throw new UserCodeExecutionException(request.id);
    }
  }

  private static class CallerThrowsTimeout implements Caller<Request, Response> {

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      throw new UserCodeTimeoutException("");
    }
  }

  private static class CallerInvokesQuotaException implements Caller<Request, Response> {

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      throw new UserCodeQuotaException(request.id);
    }
  }

  private static class SetupExceedsTimeout implements SetupTeardown {

    private final Duration timeout;

    private SetupExceedsTimeout(Duration timeout) {
      this.timeout = timeout.plus(Duration.standardSeconds(1L));
    }

    @Override
    public void setup() throws UserCodeExecutionException {
      sleep(timeout);
    }

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class SetupThrowsUserCodeExecutionException implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {
      throw new UserCodeExecutionException("error message");
    }

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class SetupThrowsUserCodeQuotaException implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {
      throw new UserCodeQuotaException("");
    }

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class SetupThrowsUserCodeTimeoutException implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {
      throw new UserCodeTimeoutException("");
    }

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class TeardownExceedsTimeout implements SetupTeardown {
    private final Duration timeout;

    private TeardownExceedsTimeout(Duration timeout) {
      this.timeout = timeout.plus(Duration.standardSeconds(1L));
    }

    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {
      sleep(timeout);
    }
  }

  private static class TeardownThrowsUserCodeExecutionException implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {
      throw new UserCodeExecutionException("");
    }
  }

  private static class TeardownThrowsUserCodeQuotaException implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {
      throw new UserCodeQuotaException("");
    }
  }

  private static class TeardownThrowsUserCodeTimeoutException implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {
      throw new UserCodeExecutionException("");
    }
  }

  private static <ErrorT extends UserCodeExecutionException> void assertPipelineThrows(
      Class<ErrorT> clazz, TestPipeline p) {

    // Because we need to wrap in a timeout via a java Future, exceptions are thrown as
    // UncheckedExecutionException
    UncheckedExecutionException error = assertThrows(UncheckedExecutionException.class, p::run);

    // Iterate through the stack trace to assert ErrorT is among stack.
    assertTrue(
        error.toString(), Throwables.getCausalChain(error).stream().anyMatch(clazz::isInstance));
  }

  private static <ErrorT extends UserCodeExecutionException> PCollection<Long> countStackTracesOf(
      PCollection<ApiIOError> failures, Class<ErrorT> clazz) {
    return failures
        .apply(
            "stackTrace " + clazz.getSimpleName(),
            MapElements.into(strings()).via(failure -> checkStateNotNull(failure).getStackTrace()))
        .apply(
            "filter " + clazz.getSimpleName(), Filter.by(input -> input.contains(clazz.getName())))
        .apply("count " + clazz.getSimpleName(), Count.globally());
  }

  private static void sleep(Duration timeout) {
    try {
      Thread.sleep(timeout.getMillis());
    } catch (InterruptedException ignored) {
    }
  }
}
