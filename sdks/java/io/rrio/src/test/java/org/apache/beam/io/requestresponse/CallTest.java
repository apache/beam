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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Call}. */
@RunWith(JUnit4.class)
public class CallTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  static final SerializableCoder<@NonNull Request> NON_DETERMINISTIC_REQUEST_CODER =
      SerializableCoder.of(Request.class);

  static final Coder<@NonNull Request> DETERMINISTIC_REQUEST_CODER =
      new DeterministicRequestCoder();

  static final SerializableCoder<@NonNull Response> NON_DETERMINISTIC_RESPONSE_CODER =
      SerializableCoder.of(Response.class);

  static final Coder<@NonNull Response> DETERMINISTIC_RESPONSE_CODER =
      new DeterministicResponseCoder();

  @Test
  public void givenCallerNotSerializable_throwsError() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Call.of(new UnSerializableCaller(), NON_DETERMINISTIC_RESPONSE_CODER));
  }

  @Test
  public void givenSetupTeardownNotSerializable_throwsError() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Call.ofCallerAndSetupTeardown(
                new UnSerializableCallerWithSetupTeardown(), NON_DETERMINISTIC_RESPONSE_CODER));
  }

  @Test
  public void givenCallerThrowsUserCodeExecutionException_emitsIntoFailurePCollection() {
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("a")))
            .apply(
                Call.of(
                    new CallerThrowsUserCodeExecutionException(),
                    NON_DETERMINISTIC_RESPONSE_CODER));

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
            .apply(Call.of(new CallerInvokesQuotaException(), NON_DETERMINISTIC_RESPONSE_CODER));

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
                Call.of(new CallerExceedsTimeout(timeout), NON_DETERMINISTIC_RESPONSE_CODER)
                    .withTimeout(timeout));

    PCollection<ApiIOError> failures = result.getFailures();
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeExecutionException.class))
        .isEqualTo(0L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeQuotaException.class)).isEqualTo(0L);
    PAssert.thatSingleton(countStackTracesOf(failures, UserCodeTimeoutException.class))
        .isEqualTo(1L);

    pipeline.run();
  }

  @Test
  public void givenCallerThrowsTimeoutException_emitsFailurePCollection() {
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("a")))
            .apply(Call.of(new CallerThrowsTimeout(), NON_DETERMINISTIC_RESPONSE_CODER));

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
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
                .withSetupTeardown(new SetupThrowsUserCodeExecutionException()));

    assertPipelineThrows(UserCodeExecutionException.class, pipeline);
  }

  @Test
  public void givenSetupThrowsQuotaException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
                .withSetupTeardown(new SetupThrowsUserCodeQuotaException()));

    assertPipelineThrows(UserCodeQuotaException.class, pipeline);
  }

  @Test
  public void givenSetupTimeout_throwsError() {
    Duration timeout = Duration.standardSeconds(1L);

    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
                .withSetupTeardown(new SetupExceedsTimeout(timeout))
                .withTimeout(timeout));

    assertPipelineThrows(UserCodeTimeoutException.class, pipeline);
  }

  @Test
  public void givenSetupThrowsTimeoutException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
                .withSetupTeardown(new SetupThrowsUserCodeTimeoutException()));

    assertPipelineThrows(UserCodeTimeoutException.class, pipeline);
  }

  @Test
  public void givenTeardownThrowsUserCodeExecutionException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
                .withSetupTeardown(new TeardownThrowsUserCodeExecutionException()));

    // Exceptions thrown during teardown do not populate with the cause
    assertThrows(IllegalStateException.class, () -> pipeline.run());
  }

  @Test
  public void givenTeardownThrowsQuotaException_throwsError() {
    pipeline
        .apply(Create.of(new Request("")))
        .apply(
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
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
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
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
            Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER)
                .withSetupTeardown(new TeardownThrowsUserCodeTimeoutException()));

    // Exceptions thrown during teardown do not populate with the cause
    assertThrows(IllegalStateException.class, () -> pipeline.run());
  }

  @Test
  public void givenValidCaller_emitValidResponse() {
    Result<Response> result =
        pipeline
            .apply(Create.of(new Request("a")))
            .apply(Call.of(new ValidCaller(), NON_DETERMINISTIC_RESPONSE_CODER));

    PAssert.thatSingleton(result.getFailures().apply(Count.globally())).isEqualTo(0L);
    PAssert.that(result.getResponses()).containsInAnyOrder(new Response("a"));

    pipeline.run();
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

  static class Request implements Serializable {

    final String id;

    Request(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
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

  static class Response implements Serializable {
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

  private static class DeterministicRequestCoder extends CustomCoder<@NonNull Request> {
    private static final Coder<String> ID_CODER = StringUtf8Coder.of();

    @Override
    public void encode(Request value, @NotNull OutputStream outStream)
        throws CoderException, IOException {
      ID_CODER.encode(checkStateNotNull(value).id, outStream);
    }

    @Override
    public @NonNull Request decode(@NotNull InputStream inStream)
        throws CoderException, IOException {
      String id = ID_CODER.decode(inStream);
      return new Request(id);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      ID_CODER.verifyDeterministic();
    }
  }

  private static class DeterministicResponseCoder extends CustomCoder<Response> {
    private static final NullableCoder<String> ID_CODER = NullableCoder.of(StringUtf8Coder.of());

    @Override
    public void encode(@Nullable Response value, @NotNull OutputStream outStream)
        throws CoderException, IOException {
      if (value == null) {
        ID_CODER.encode(null, outStream);
        return;
      }
      ID_CODER.encode(checkStateNotNull(value).id, outStream);
    }

    @Override
    public Response decode(@NotNull InputStream inStream) throws CoderException, IOException {
      try {
        String id = ID_CODER.decode(inStream);
        return new Response(id);
      } catch (CoderException ignored) {
        return null;
      }
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      ID_CODER.verifyDeterministic();
    }
  }
}
