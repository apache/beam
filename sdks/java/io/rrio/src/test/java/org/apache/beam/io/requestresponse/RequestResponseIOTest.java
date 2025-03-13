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
import static org.hamcrest.Matchers.greaterThan;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RequestResponseIO}. */
@RunWith(JUnit4.class)
public class RequestResponseIOTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static final TypeDescriptor<Response> RESPONSE_TYPE = TypeDescriptor.of(Response.class);
  private static final SchemaProvider SCHEMA_PROVIDER = new AutoValueSchema();

  private static final Coder<Response> RESPONSE_CODER =
      SchemaCoder.of(
          checkStateNotNull(SCHEMA_PROVIDER.schemaFor(RESPONSE_TYPE)),
          RESPONSE_TYPE,
          checkStateNotNull(SCHEMA_PROVIDER.toRowFunction(RESPONSE_TYPE)),
          checkStateNotNull(SCHEMA_PROVIDER.fromRowFunction(RESPONSE_TYPE)));

  @Test
  public void givenCallerOnly_thenProcessesRequestsWithDefaultFeatures() {
    Caller<Request, Response> caller = new CallerImpl();

    RequestResponseIO<Request, Response> transform =
        RequestResponseIO.of(caller, RESPONSE_CODER)
            .withMonitoringConfiguration(
                Monitoring.builder().build().withEverythingCountedExceptedCaching());

    Result<Response> result = requests().apply("rrio", transform);

    PAssert.that(result.getFailures()).empty();
    PAssert.that(result.getResponses()).containsInAnyOrder(responses());

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();

    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.REQUESTS_COUNTER_NAME), greaterThan(0L));
    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.RESPONSES_COUNTER_NAME), greaterThan(0L));

    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.callCounterNameOf(caller)),
        greaterThan(0L));

    assertThat(
        getCounterResult(
            metrics, Call.class, Monitoring.setupCounterNameOf(new Call.NoopSetupTeardown())),
        greaterThan(0L));

    // We expect remaining metrics to be 0.
    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.FAILURES_COUNTER_NAME), equalTo(0L));

    assertThat(
        getCounterResult(
            metrics,
            Call.class,
            Monitoring.shouldBackoffCounterName(
                transform.getCallConfiguration().getCallShouldBackoff())),
        equalTo(0L));

    assertThat(
        getCounterResult(
            metrics,
            Call.class,
            Monitoring.sleeperCounterNameOf(
                transform.getCallConfiguration().getSleeperSupplier().get())),
        equalTo(0L));

    assertThat(
        getCounterResult(
            metrics,
            Call.class,
            Monitoring.backoffCounterNameOf(
                transform.getCallConfiguration().getBackOffSupplier().get())),
        equalTo(0L));
  }

  @Test
  public void givenCallerAndSetupTeardown_thenCallerInvokesSetupTeardown() {
    Result<Response> result =
        requests()
            .apply(
                "rrio",
                RequestResponseIO.ofCallerAndSetupTeardown(
                        new CallerSetupTeardownImpl(), RESPONSE_CODER)
                    .withMonitoringConfiguration(
                        Monitoring.builder().setCountCalls(true).setCountSetup(true).build()));

    PAssert.that(result.getFailures()).empty();
    PAssert.that(result.getResponses()).containsInAnyOrder(responses());

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metricResults = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();

    assertThat(
        getCounterResult(
            metricResults, Call.class, Monitoring.callCounterNameOf(new CallerSetupTeardownImpl())),
        greaterThan(0L));

    assertThat(
        getCounterResult(
            metricResults,
            Call.class,
            Monitoring.setupCounterNameOf(new CallerSetupTeardownImpl())),
        greaterThan(0L));
  }

  @Test
  public void givenDefaultConfiguration_shouldRepeatFailedRequests() {
    Result<Response> result =
        requests()
            .apply(
                "rrio",
                RequestResponseIO.of(new CallerImpl(1), RESPONSE_CODER)
                    .withMonitoringConfiguration(Monitoring.builder().setCountCalls(true).build()));

    PAssert.that(result.getFailures()).empty();
    PAssert.that(result.getResponses()).containsInAnyOrder(responses());

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.callCounterNameOf(new CallerImpl())),
        equalTo(2L));
  }

  @Test
  public void givenDefaultConfiguration_usesDefaultBackoffSupplier() {
    Caller<Request, Response> caller = new CallerImpl(1);

    requests()
        .apply(
            "rrio",
            RequestResponseIO.of(caller, RESPONSE_CODER)
                .withMonitoringConfiguration(Monitoring.builder().setCountBackoffs(true).build()));

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
        getCounterResult(
            metrics,
            Call.class,
            Monitoring.backoffCounterNameOf(new DefaultSerializableBackoffSupplier().get())),
        greaterThan(0L));
  }

  @Test
  public void givenDefaultConfiguration_usesDefaultSleeper() {
    Caller<Request, Response> caller = new CallerImpl(1);

    requests()
        .apply(
            "rrio",
            RequestResponseIO.of(caller, RESPONSE_CODER)
                .withMonitoringConfiguration(Monitoring.builder().setCountSleeps(true).build()));

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.sleeperCounterNameOf(Sleeper.DEFAULT)),
        greaterThan(0L));
  }

  @Test
  public void givenDefaultConfiguration_usesDefaultCallShouldBackoff() {
    Caller<Request, Response> caller = new CallerImpl();

    RequestResponseIO<Request, Response> transform = RequestResponseIO.of(caller, RESPONSE_CODER);
    // We prime the default implementation so that we guarantee value() == true during the test.
    CallShouldBackoffBasedOnRejectionProbability<Response> shouldBackoffImpl =
        (CallShouldBackoffBasedOnRejectionProbability<Response>)
            transform.getCallConfiguration().getCallShouldBackoff();
    shouldBackoffImpl.setThreshold(0);
    shouldBackoffImpl.update(new UserCodeExecutionException(""));

    requests()
        .apply(
            "rrio",
            transform
                .withCallShouldBackoff(shouldBackoffImpl)
                .withMonitoringConfiguration(
                    Monitoring.builder().setCountShouldBackoff(true).build()));

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
        getCounterResult(
            metrics, Call.class, Monitoring.shouldBackoffCounterName(shouldBackoffImpl)),
        greaterThan(0L));
  }

  @Test
  public void givenWithoutRepeater_shouldNotRepeatRequests() {
    Result<Response> result =
        requests()
            .apply(
                "rrio",
                RequestResponseIO.of(new CallerImpl(1), RESPONSE_CODER)
                    .withoutRepeater()
                    .withMonitoringConfiguration(
                        Monitoring.builder().setCountCalls(true).setCountFailures(true).build()));

    PAssert.that(result.getResponses()).empty();

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();

    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.callCounterNameOf(new CallerImpl())),
        greaterThan(0L));

    assertThat(
        getCounterResult(metrics, Call.class, Monitoring.FAILURES_COUNTER_NAME), greaterThan(0L));
  }

  @Test
  public void givenCustomCallShouldBackoff_thenComputeUsingCustom() {
    CustomCallShouldBackoff<Response> customCallShouldBackoff = new CustomCallShouldBackoff<>();
    requests()
        .apply(
            "rrio",
            RequestResponseIO.of(new CallerImpl(), RESPONSE_CODER)
                .withCallShouldBackoff(customCallShouldBackoff));

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();

    assertThat(
        getCounterResult(
            metrics,
            customCallShouldBackoff.getClass(),
            customCallShouldBackoff.getCounterName().getName()),
        greaterThan(0L));
  }

  @Test
  public void givenCustomSleeper_thenSleepBehaviorCustom() {
    CustomSleeperSupplier customSleeperSupplier = new CustomSleeperSupplier();
    requests()
        .apply(
            "rrio",
            RequestResponseIO.of(new CallerImpl(100), RESPONSE_CODER)
                .withSleeperSupplier(customSleeperSupplier));

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
        getCounterResult(
            metrics,
            customSleeperSupplier.getClass(),
            customSleeperSupplier.getCounterName().getName()),
        greaterThan(0L));
  }

  @Test
  public void givenCustomBackoff_thenBackoffBehaviorCustom() {
    CustomBackOffSupplier customBackOffSupplier = new CustomBackOffSupplier();
    requests()
        .apply(
            "rrio",
            RequestResponseIO.of(new CallerImpl(100), RESPONSE_CODER)
                .withBackOffSupplier(customBackOffSupplier));

    PipelineResult pipelineResult = pipeline.run();
    MetricResults metrics = pipelineResult.metrics();
    pipelineResult.waitUntilFinish();
    assertThat(
        getCounterResult(
            metrics,
            customBackOffSupplier.getClass(),
            customBackOffSupplier.getCounterName().getName()),
        greaterThan(0L));
  }

  // TODO(damondouglas): Count metrics of caching after https://github.com/apache/beam/issues/29888
  // resolves.
  @Ignore
  @Test
  public void givenWithCache_thenRequestsResponsesCachedUsingCustom() {}

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

    private CallerImpl() {}

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
    private final Counter counter =
        Metrics.counter(CustomCallShouldBackoff.class, "custom_counter");

    @Override
    public void update(UserCodeExecutionException exception) {}

    @Override
    public void update(ResponseT response) {}

    @Override
    public boolean isTrue() {
      counter.inc();
      return false;
    }

    MetricName getCounterName() {
      return counter.getName();
    }
  }

  private static class CustomSleeperSupplier implements SerializableSupplier<Sleeper> {
    private final Counter counter = Metrics.counter(CustomSleeperSupplier.class, "custom_counter");

    @Override
    public Sleeper get() {
      return millis -> counter.inc();
    }

    MetricName getCounterName() {
      return counter.getName();
    }
  }

  private static class CustomBackOffSupplier implements SerializableSupplier<BackOff> {

    private final Counter counter = Metrics.counter(CustomBackOffSupplier.class, "custom_counter");

    @Override
    public BackOff get() {
      return new BackOff() {
        @Override
        public void reset() throws IOException {}

        @Override
        public long nextBackOffMillis() throws IOException {
          counter.inc();
          return 0;
        }
      };
    }

    MetricName getCounterName() {
      return counter.getName();
    }
  }

  private static Long getCounterResult(MetricResults metrics, Class<?> namespace, String name) {
    MetricQueryResults metricQueryResults =
        metrics.queryMetrics(
            MetricsFilter.builder().addNameFilter(MetricNameFilter.named(namespace, name)).build());
    return getCounterResult(metricQueryResults.getCounters(), namespace, name);
  }

  private static Long getCounterResult(
      Iterable<MetricResult<Long>> counters, Class<?> namespace, String name) {
    Long result = 0L;
    for (MetricResult<Long> counter : counters) {
      MetricName metricName = counter.getName();
      if (metricName.getNamespace().equals(namespace.getName())
          && metricName.getName().equals(name)) {
        result = counter.getCommitted();
      }
    }
    return result;
  }
}
