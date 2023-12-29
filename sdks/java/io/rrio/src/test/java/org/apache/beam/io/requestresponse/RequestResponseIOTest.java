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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RequestResponseIO}. */
@RunWith(JUnit4.class)
public class RequestResponseIOTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static final TypeDescriptor<Request> REQUEST_TYPE = TypeDescriptor.of(Request.class);
  private static final TypeDescriptor<Response> RESPONSE_TYPE = TypeDescriptor.of(Response.class);
  private static final SchemaProvider SCHEMA_PROVIDER = new AutoValueSchema();

  private static final Coder<Request> REQUEST_CODER =
      SchemaCoder.of(
          checkStateNotNull(SCHEMA_PROVIDER.schemaFor(REQUEST_TYPE)),
          REQUEST_TYPE,
          checkStateNotNull(SCHEMA_PROVIDER.toRowFunction(REQUEST_TYPE)),
          checkStateNotNull(SCHEMA_PROVIDER.fromRowFunction(REQUEST_TYPE)));

  private static final Coder<Response> RESPONSE_CODER =
      SchemaCoder.of(
          checkStateNotNull(SCHEMA_PROVIDER.schemaFor(RESPONSE_TYPE)),
          RESPONSE_TYPE,
          checkStateNotNull(SCHEMA_PROVIDER.toRowFunction(RESPONSE_TYPE)),
          checkStateNotNull(SCHEMA_PROVIDER.fromRowFunction(RESPONSE_TYPE)));

  @Test
  public void givenCallerOnly_thenProcessesRequestsOnly() {
    Result<Response> result =
        requests()
            .apply(
                "rrio",
                RequestResponseIO.of(new CallerImpl(), RESPONSE_CODER)
                    .withMonitoringConfiguration(Monitoring.builder().setCountCalls(true)
                            .setCountSetup(true)
                            .build()));

    PAssert.that(result.getFailures()).empty();
    PAssert.that(result.getResponses()).containsInAnyOrder(responses());

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
        getCounterResult(metrics, Call.class, CallerImpl.class.getSimpleName() + "_call"),
        equalTo(1L));

    assertThat(
            getCounterResult(metrics, Call.class, Call.NoopSetupTeardown.class.getSimpleName() + "_setup"),
            equalTo(1L));
  }

  @Test
  public void givenCallerAndSetupTeardown_thenCallerInvokesSetupTeardown() {
    Result<Response> result =
            requests().apply("rrio", RequestResponseIO.ofCallerAndSetupTeardown(new CallerSetupTeardownImpl(), RESPONSE_CODER)
                    .withMonitoringConfiguration(Monitoring.builder()
                            .setCountCalls(true)
                            .setCountSetup(true)
                            .build()));

    PAssert.that(result.getFailures()).empty();
    PAssert.that(result.getResponses()).containsInAnyOrder(responses());

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metricResults = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    String counterNamePrefix = CallerSetupTeardownImpl.class.getSimpleName();
    assertThat(getCounterResult(metricResults, Call.class, counterNamePrefix + "_call"), equalTo(1L));
    assertThat(getCounterResult(metricResults, Call.class, counterNamePrefix + "_setup"), equalTo(1L));
  }

  @Test
  public void givenDefaultConfiguration_shouldRepeatFailedRequests() {
    Result<Response> result =
            requests().apply("rrio", RequestResponseIO.of(new CallerImpl(1), RESPONSE_CODER)
                    .withMonitoringConfiguration(Monitoring.builder()
                            .setCountCalls(true)
                            .build()));

    PAssert.that(result.getFailures()).empty();
    PAssert.that(result.getResponses()).containsInAnyOrder(responses());

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
            getCounterResult(metrics, Call.class, CallerImpl.class.getSimpleName() + "_call"),
            equalTo(2L));

    assertThat(
            getCounterResult(metrics, Call.class, Call.NoopSetupTeardown.class.getSimpleName() + "_setup"),
            equalTo(1L));
  }

  @Test
  public void givenDefaultConfiguration_usesDefaultBackoffSupplier() {}

  @Test
  public void givenDefaultConfiguration_usesDefaultSleeper() {}

  @Test
  public void givenDefaultConfiguration_usesDefaultCallShouldBackoff() {}

  @Test
  public void givenWithTimeout_overridesDefaultTimeout() {}

  @Test
  public void givenWithoutRepeater_shouldNotRepeatRequests() {}

  @Test
  public void givenCustomCallShouldBackoff_thenShouldBackoffEvaluationCustom() {}

  @Test
  public void givenCustomSleeper_thenSleepBehaviorCustom() {}

  @Test
  public void givenCustomBackoff_thenBackoffBehaviorCustom() {}

  @Ignore
  @Test
  public void givenWithCache_thenRequestsResponsesCached() {
    System.out.println(Request.builder().build());
    System.out.println(Response.builder().build());
    System.out.println(new CallerSetupTeardownImpl());
    System.out.println(new CallerImpl());
    System.out.println(new CustomSleeperSupplier());
    System.out.println(new CustomBackOffSupplier());
    System.out.println(new CustomCallShouldBackoff<Response>());
    System.out.println(REQUEST_CODER);
  }

  private PCollection<Request> requests() {
    return pipeline.apply(
        "create requests", Create.of(Request.builder().setALong(1L).setAString("a").build()));
  }

  private List<Response> responses() {
    return ImmutableList.of(Response.builder().setAString("a").setALong(1L).build());
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Request {

    static Builder builder() {
      return new AutoValue_RequestResponseIOTest_Request.Builder();
    }

    abstract String getAString();

    abstract Long getALong();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setAString(String value);

      abstract Builder setALong(Long value);

      abstract Request build();
    }
  }

  @AutoValue
  abstract static class Response {
    static Builder builder() {
      return new AutoValue_RequestResponseIOTest_Response.Builder();
    }

    abstract String getAString();

    abstract Long getALong();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setAString(String value);

      abstract Builder setALong(Long value);

      abstract Response build();
    }
  }

  private static class CallerSetupTeardownImpl implements Caller<Request, Response>, SetupTeardown {
    private final CallerImpl caller = new CallerImpl();

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      return caller.call(request);
    }

    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class CallerImpl implements Caller<Request, Response> {
    private int numErrors = 0;
    private CallerImpl(){}
    private CallerImpl(int numErrors) {
      this.numErrors = numErrors;
    }

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      if (numErrors > 0) {
        numErrors--;
        throw new UserCodeQuotaException("");
      }
      return Response.builder()
          .setAString(request.getAString())
          .setALong(request.getALong())
          .build();
    }
  }

  private static class CustomCallShouldBackoff<ResponseT> implements CallShouldBackoff<ResponseT> {

    @Override
    public void update(UserCodeExecutionException exception) {}

    @Override
    public void update(ResponseT response) {}

    @Override
    public boolean value() {
      return false;
    }
  }

  private static class CustomSleeperSupplier implements SerializableSupplier<Sleeper> {

    @Override
    public Sleeper get() {
      return millis -> {};
    }
  }

  private static class CustomBackOffSupplier implements SerializableSupplier<BackOff> {

    @Override
    public BackOff get() {
      return new BackOff() {
        @Override
        public void reset() throws IOException {}

        @Override
        public long nextBackOffMillis() throws IOException {
          return 0;
        }
      };
    }
  }

  private static Long getCounterResult(MetricResults metrics, Class<?> clazz, String name) {
    MetricQueryResults metricQueryResults =
        metrics.queryMetrics(
            MetricsFilter.builder().addNameFilter(MetricNameFilter.named(clazz, name)).build());
    return getCounterResult(metricQueryResults.getCounters(), clazz, name);
  }

  private static Long getCounterResult(
      Iterable<MetricResult<Long>> counters, Class<?> clazz, String name) {
    Long result = 0L;
    for (MetricResult<Long> counter : counters) {
      MetricName metricName = counter.getName();
      if (metricName.getNamespace().equals(clazz.getName()) && metricName.getName().equals(name)) {
        result = counter.getCommitted();
      }
    }
    return result;
  }
}
