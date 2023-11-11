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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

class ThrottleWithExternalResource<
        @NonNull T,
        ReporterT extends Caller<@NonNull String, @NonNull Long> & SetupTeardown,
        EnqueuerT extends Caller<@NonNull T, Void> & SetupTeardown,
        DequeuerT extends Caller<@NonNull Instant, @NonNull T> & SetupTeardown,
        RefresherT extends Caller<@NonNull Instant, Void> & SetupTeardown>
    extends PTransform<@NonNull PCollection<@NonNull T>, Call.@NonNull Result<@NonNull T>> {

  /** Instantiate a {@link ThrottleWithExternalResource} using a {@link RedisClient}. */
  static <@NonNull T>
      ThrottleWithExternalResource<
              @NonNull T,
              RedisReporter,
              RedisEnqueuer<@NonNull T>,
              RedisDequeur<@NonNull T>,
              RedisRefresher>
          usingRedis(
              URI uri,
              String quotaIdentifier,
              String queueKey,
              Quota quota,
              Coder<@NonNull T> coder)
              throws Coder.NonDeterministicException {
    return new ThrottleWithExternalResource<
        @NonNull T,
        RedisReporter,
        RedisEnqueuer<@NonNull T>,
        RedisDequeur<@NonNull T>,
        RedisRefresher>(
        quota,
        quotaIdentifier,
        coder,
        new RedisReporter(uri),
        new RedisEnqueuer<>(uri, queueKey, coder),
        new RedisDequeur<>(uri, coder, queueKey),
        new RedisRefresher(uri, quota, quotaIdentifier));
  }

  private static final Duration THROTTLE_INTERVAL = Duration.standardSeconds(1L);

  private final @NonNull Quota quota;
  private final @NonNull String quotaIdentifier;
  private final @NonNull Coder<@NonNull T> coder;
  private final @NonNull ReporterT reporterT;
  private final @NonNull EnqueuerT enqueuerT;
  private final @NonNull DequeuerT dequeuerT;
  private final @NonNull RefresherT refresherT;

  ThrottleWithExternalResource(
      @NonNull Quota quota,
      @NonNull String quotaIdentifier,
      @NonNull Coder<@NonNull T> coder,
      @NonNull ReporterT reporterT,
      @NonNull EnqueuerT enqueuerT,
      @NonNull DequeuerT dequeuerT,
      @NonNull RefresherT refresherT)
      throws Coder.NonDeterministicException {
    this.quotaIdentifier = quotaIdentifier;
    this.reporterT = reporterT;
    coder.verifyDeterministic();
    checkArgument(!quotaIdentifier.isEmpty());
    this.quota = quota;
    this.coder = coder;
    this.enqueuerT = enqueuerT;
    this.dequeuerT = dequeuerT;
    this.refresherT = refresherT;
  }

  @Override
  public Call.@NonNull Result<@NonNull T> expand(PCollection<@NonNull T> input) {
    Pipeline pipeline = input.getPipeline();

    // Refresh known quota to control the throttle rate.
    Call.Result<Void> refreshResult =
        pipeline
            .apply("quota/impulse", PeriodicImpulse.create().withInterval(quota.getInterval()))
            .apply("quota/refresh", getRefresher());

    // Enqueue T elements.
    Call.Result<Void> enqueuResult = input.apply("enqueue", getEnqueuer());

    TupleTag<@NonNull T> outputTag = new TupleTag<@NonNull T>() {};
    TupleTag<@NonNull ApiIOError> failureTag = new TupleTag<@NonNull ApiIOError>() {};

    // Perform Throttle.
    PCollectionTuple pct =
        pipeline
            .apply("throttle/impulse", PeriodicImpulse.create().withInterval(THROTTLE_INTERVAL))
            .apply(
                "throttle/fn",
                ParDo.of(
                        new ThrottleFn(
                            quotaIdentifier, dequeuerT, reporterT, outputTag, failureTag))
                    .withOutputTags(outputTag, TupleTagList.of(failureTag)));

    PCollection<ApiIOError> errors =
        PCollectionList.of(refreshResult.getFailures())
            .and(enqueuResult.getFailures())
            .and(pct.get(failureTag))
            .apply("errors/flatten", Flatten.pCollections());

    TupleTag<@NonNull T> resultOutputTag = new TupleTag<@NonNull T>() {};
    TupleTag<@NonNull ApiIOError> resultFailureTag = new TupleTag<@NonNull ApiIOError>() {};

    return Call.Result.<@NonNull T>of(
        coder,
        resultOutputTag,
        resultFailureTag,
        PCollectionTuple.of(resultOutputTag, pct.get(outputTag)).and(resultFailureTag, errors));
  }

  private Call<@NonNull Instant, Void> getRefresher() {
    return Call.ofCallerAndSetupTeardown(refresherT, VoidCoder.of());
  }

  private Call<@NonNull T, Void> getEnqueuer() {
    return Call.ofCallerAndSetupTeardown(enqueuerT, VoidCoder.of());
  }

  private class ThrottleFn extends DoFn<@NonNull Instant, @NonNull T> {
    private final String quotaIdentifier;
    private final DequeuerT dequeuerT;
    private final ReporterT reporterT;
    private final TupleTag<@NonNull T> outputTag;
    private final TupleTag<@NonNull ApiIOError> failureTag;

    private ThrottleFn(
        String quotaIdentifier,
        DequeuerT dequeuerT,
        ReporterT reporterT,
        TupleTag<@NonNull T> outputTag,
        TupleTag<@NonNull ApiIOError> failureTag) {
      this.quotaIdentifier = quotaIdentifier;
      this.dequeuerT = dequeuerT;
      this.reporterT = reporterT;
      this.outputTag = outputTag;
      this.failureTag = failureTag;
    }

    @ProcessElement
    public void process(@Element @NonNull Instant instant, MultiOutputReceiver receiver) {
      // Check for available quota.
      try {
        if (reporterT.call(quotaIdentifier) <= 0L) {
          return;
        }

        // Dequeue an element if quota available.
        T element = dequeuerT.call(instant);
        // Emit element.
        receiver.get(outputTag).output(element);
      } catch (UserCodeExecutionException e) {
        receiver
            .get(failureTag)
            .output(
                ApiIOError.builder()
                    .setRequestAsJsonString(String.format("{\"request\": \"%s\"}", instant))
                    .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                    .setObservedTimestamp(Instant.now())
                    .setStackTrace(Throwables.getStackTraceAsString(e))
                    .build());
      }
    }

    @Setup
    public void setup() throws UserCodeExecutionException {
      enqueuerT.setup();
      dequeuerT.setup();
      reporterT.setup();
    }

    @Teardown
    public void teardown() throws UserCodeExecutionException {
      enqueuerT.teardown();
      dequeuerT.teardown();
      reporterT.teardown();
    }
  }

  private static class RedisReporter extends RedisSetupTeardown
      implements Caller<@NonNull String, @NonNull Long> {
    private RedisReporter(URI uri) {
      super(new RedisClient(uri));
    }

    @Override
    public @NonNull Long call(@NonNull String request) throws UserCodeExecutionException {
      return client.getLong(request);
    }
  }

  private static class RedisEnqueuer<@NonNull T> extends RedisSetupTeardown
      implements Caller<@NonNull T, Void> {
    private final String key;
    private final Coder<@NonNull T> coder;

    private RedisEnqueuer(URI uri, String key, Coder<@NonNull T> coder) {
      super(new RedisClient(uri));
      this.key = key;
      this.coder = coder;
    }

    @Override
    public Void call(@NonNull T request) throws UserCodeExecutionException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        coder.encode(request, baos);
      } catch (IOException e) {
        throw new UserCodeExecutionException(e);
      }
      client.rpush(key, baos.toByteArray());
      return null;
    }
  }

  private static class RedisDequeur<@NonNull T> extends RedisSetupTeardown
      implements Caller<@NonNull Instant, @NonNull T> {

    private final Coder<@NonNull T> coder;
    private final String key;

    private RedisDequeur(URI uri, Coder<@NonNull T> coder, String key) {
      super(new RedisClient(uri));
      this.coder = coder;
      this.key = key;
    }

    @Override
    public @NonNull T call(@NonNull Instant request) throws UserCodeExecutionException {
      byte[] bytes = client.lpop(key);
      try {
        return checkStateNotNull(coder.decode(ByteSource.wrap(bytes).openStream()));

      } catch (IOException e) {
        throw new UserCodeExecutionException(e);
      }
    }
  }

  private static class RedisRefresher extends RedisSetupTeardown
      implements Caller<@NonNull Instant, Void> {
    private final Quota quota;
    private final String key;

    private RedisRefresher(URI uri, Quota quota, String key) {
      super(new RedisClient(uri));
      this.quota = quota;
      this.key = key;
    }

    @Override
    public Void call(@NonNull Instant request) throws UserCodeExecutionException {
      client.setex(key, quota.getNumRequests(), quota.getInterval());
      return null;
    }
  }

  private abstract static class RedisSetupTeardown implements SetupTeardown {
    protected final RedisClient client;

    private RedisSetupTeardown(RedisClient client) {
      this.client = client;
    }

    @Override
    public void setup() throws UserCodeExecutionException {
      client.setup();
    }

    @Override
    public void teardown() throws UserCodeExecutionException {
      client.teardown();
    }
  }
}
