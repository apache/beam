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
import java.util.ArrayList;
import java.util.List;
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
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Throttles a {@link T} {@link PCollection} using an external resource.
 *
 * <p>{@link ThrottleWithExternalResource} makes use of {@link PeriodicImpulse} as it needs to
 * coordinate three {@link PTransform}s concurrently. Usage of {@link ThrottleWithExternalResource}
 * should consider the impact of {@link PeriodicImpulse} on the pipeline.
 *
 * <p>Usage of {@link ThrottleWithExternalResource} is completely optional and serves as one of many
 * methods by {@link RequestResponseIO} to protect against API overuse. Usage should not depend on
 * {@link ThrottleWithExternalResource} alone to achieve API overuse prevention for several reasons.
 * The underlying external resource may not scale at all or as fast as a Beam Runner. The external
 * resource itself may be an API with its own quota that {@link ThrottleWithExternalResource} does
 * not consider.
 *
 * <p>{@link ThrottleWithExternalResource} makes use of several {@link Caller}s that work together
 * to achieve its aim of throttling a {@link T} {@link PCollection}. A {@link RefresherT} is a
 * {@link Caller} that takes an {@link Instant} and refreshes a shared {@link Quota}. An {@link
 * EnqueuerT} enqueues a {@link T} element while a {@link DequeuerT} dequeues said element when the
 * {@link ReporterT} reports that the stored {@link Quota#getNumRequests} is >0. Finally, a {@link
 * DecrementerT} decrements from the shared {@link Quota} value, additionally reporting the value
 * after performing the action.
 *
 * <p>{@link ThrottleWithExternalResource} instantiates and applies two {@link Call} {@link
 * PTransform}s using the aforementioned {@link Caller}s {@link RefresherT} and {@link EnqueuerT}.
 * {@link ThrottleWithExternalResource} calls {@link ReporterT}, {@link DequeuerT}, {@link
 * DecrementerT} within its {@link DoFn}, emitting the dequeued {@link T} when the {@link ReporterT}
 * reports a value >0. As an additional safety check, the DoFn checks whether the {@link Quota}
 * value after {@link DecrementerT}'s action is <0, signaling that multiple workers are attempting
 * the same too fast and therefore exists the DoFn allowing for the next refresh.
 *
 * <p>{@link ThrottleWithExternalResource} flattens errors emitted from {@link EnqueuerT}, {@link
 * RefresherT}, and its own {@link DoFn} into a single {@link ApiIOError} {@link PCollection} that
 * is encapsulated, with a {@link T} {@link PCollection} output into a {@link Call.Result}.
 */
class ThrottleWithExternalResource<
        T,
        ReporterT extends Caller<String, Long> & SetupTeardown,
        EnqueuerT extends Caller<T, Void> & SetupTeardown,
        DequeuerT extends Caller<Instant, T> & SetupTeardown,
        DecrementerT extends Caller<Instant, Long> & SetupTeardown,
        RefresherT extends Caller<Instant, Void> & SetupTeardown>
    extends PTransform<PCollection<T>, Call.Result<T>> {

  /**
   * Instantiate a {@link ThrottleWithExternalResource} using a {@link RedisClient}.
   *
   * <p><a href="https://redis.io">Redis</a> is designed for multiple workloads, simultaneously
   * reading and writing to a shared instance. See <a
   * href="https://redis.io/docs/get-started/faq/">Redis FAQ</a> for more information on important
   * considerations when using Redis as {@link ThrottleWithExternalResource}'s external resource.
   */
  static <T>
      ThrottleWithExternalResource<
              T,
              RedisReporter,
              RedisEnqueuer<T>,
              RedisDequeuer<T>,
              RedisDecrementer,
              RedisRefresher>
          usingRedis(URI uri, String quotaIdentifier, String queueKey, Quota quota, Coder<T> coder)
              throws Coder.NonDeterministicException {
    return new ThrottleWithExternalResource<
        T, RedisReporter, RedisEnqueuer<T>, RedisDequeuer<T>, RedisDecrementer, RedisRefresher>(
        quota,
        quotaIdentifier,
        coder,
        new RedisReporter(uri),
        new RedisEnqueuer<>(uri, queueKey, coder),
        new RedisDequeuer<>(uri, coder, queueKey),
        new RedisDecrementer(uri, queueKey),
        new RedisRefresher(uri, quota, quotaIdentifier));
  }

  private static final Duration THROTTLE_INTERVAL = Duration.standardSeconds(1L);

  private final Quota quota;
  private final String quotaIdentifier;
  private final Coder<T> coder;
  private final ReporterT reporterT;
  private final EnqueuerT enqueuerT;
  private final DequeuerT dequeuerT;
  private final DecrementerT decrementerT;
  private final RefresherT refresherT;

  ThrottleWithExternalResource(
      Quota quota,
      String quotaIdentifier,
      Coder<T> coder,
      ReporterT reporterT,
      EnqueuerT enqueuerT,
      DequeuerT dequeuerT,
      DecrementerT decrementerT,
      RefresherT refresherT)
      throws Coder.NonDeterministicException {
    this.quotaIdentifier = quotaIdentifier;
    this.reporterT = reporterT;
    coder.verifyDeterministic();
    checkArgument(!quotaIdentifier.isEmpty());
    this.quota = quota;
    this.coder = coder;
    this.enqueuerT = enqueuerT;
    this.dequeuerT = dequeuerT;
    this.decrementerT = decrementerT;
    this.refresherT = refresherT;
  }

  @Override
  public Call.Result<T> expand(PCollection<T> input) {
    Pipeline pipeline = input.getPipeline();

    // Refresh known quota to control the throttle rate.
    Call.Result<Void> refreshResult =
        pipeline
            .apply("quota impulse", PeriodicImpulse.create().withInterval(quota.getInterval()))
            .apply("quota refresh", getRefresher());

    // Enqueue T elements.
    Call.Result<Void> enqueuResult = input.apply("enqueue", getEnqueuer());

    TupleTag<T> outputTag = new TupleTag<T>() {};
    TupleTag<ApiIOError> failureTag = new TupleTag<ApiIOError>() {};

    // Perform Throttle.
    PCollectionTuple pct =
        pipeline
            .apply("throttle impulse", PeriodicImpulse.create().withInterval(THROTTLE_INTERVAL))
            .apply(
                "throttle fn",
                ParDo.of(
                        new ThrottleFn(
                            quotaIdentifier,
                            dequeuerT,
                            decrementerT,
                            reporterT,
                            outputTag,
                            failureTag))
                    .withOutputTags(outputTag, TupleTagList.of(failureTag)));

    PCollection<ApiIOError> errors =
        PCollectionList.of(refreshResult.getFailures())
            .and(enqueuResult.getFailures())
            .and(pct.get(failureTag))
            .apply("errors flatten", Flatten.pCollections());

    TupleTag<T> resultOutputTag = new TupleTag<T>() {};
    TupleTag<ApiIOError> resultFailureTag = new TupleTag<ApiIOError>() {};

    return Call.Result.<T>of(
        coder,
        resultOutputTag,
        resultFailureTag,
        PCollectionTuple.of(resultOutputTag, pct.get(outputTag)).and(resultFailureTag, errors));
  }

  private Call<Instant, Void> getRefresher() {
    return Call.ofCallerAndSetupTeardown(refresherT, VoidCoder.of());
  }

  private Call<T, Void> getEnqueuer() {
    return Call.ofCallerAndSetupTeardown(enqueuerT, VoidCoder.of());
  }

  private class ThrottleFn extends DoFn<Instant, T> {
    private final String quotaIdentifier;
    private final DequeuerT dequeuerT;
    private final DecrementerT decrementerT;
    private final ReporterT reporterT;
    private final TupleTag<T> outputTag;
    private final TupleTag<ApiIOError> failureTag;

    private ThrottleFn(
        String quotaIdentifier,
        DequeuerT dequeuerT,
        DecrementerT decrementerT,
        ReporterT reporterT,
        TupleTag<T> outputTag,
        TupleTag<ApiIOError> failureTag) {
      this.quotaIdentifier = quotaIdentifier;
      this.dequeuerT = dequeuerT;
      this.decrementerT = decrementerT;
      this.reporterT = reporterT;
      this.outputTag = outputTag;
      this.failureTag = failureTag;
    }

    @ProcessElement
    public void process(@Element Instant instant, MultiOutputReceiver receiver) {
      // Check for available quota.
      try {
        if (reporterT.call(quotaIdentifier) <= 0L) {
          return;
        }

        // Decrement the quota.
        Long quotaAfterDecrement = decrementerT.call(instant);

        // As an additional protection we check what the quota is after decrementing. A value
        // < 0 signals that multiple simultaneous workers have attempted to decrement too quickly.
        // We don't bother adding the quota back to prevent additional workers from doing the same
        // and just wait for the next refresh, exiting the DoFn.
        if (quotaAfterDecrement < 0) {
          return;
        }

        // Dequeue an element if quota available. An error here would not result in loss of data
        // as no element would successfully dequeue from the external resource but instead throw.
        T element = dequeuerT.call(instant);

        // Finally, emit the element.
        receiver.get(outputTag).output(element);

      } catch (UserCodeExecutionException e) {
        receiver
            .get(failureTag)
            .output(
                ApiIOError.builder()
                    // no request to emit as part of the error.
                    .setRequestAsJsonString("")
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
      decrementerT.setup();
      reporterT.setup();
    }

    @Teardown
    public void teardown() throws UserCodeExecutionException {
      List<String> messages = new ArrayList<>();
      String format = "%s encountered error during teardown: %s";
      try {
        enqueuerT.teardown();
      } catch (UserCodeExecutionException e) {
        messages.add(String.format(format, "enqueuerT", e));
      }
      try {
        dequeuerT.teardown();
      } catch (UserCodeExecutionException e) {
        messages.add(String.format(format, "dequeuerT", e));
      }
      try {
        decrementerT.teardown();
      } catch (UserCodeExecutionException e) {
        messages.add(String.format(format, "decrementerT", e));
      }
      try {
        reporterT.teardown();
      } catch (UserCodeExecutionException e) {
        messages.add(String.format(format, "reporterT", e));
      }

      if (!messages.isEmpty()) {
        throw new UserCodeExecutionException(String.join("; ", messages));
      }
    }
  }

  private static class RedisReporter extends RedisSetupTeardown implements Caller<String, Long> {
    private RedisReporter(URI uri) {
      super(new RedisClient(uri));
    }

    @Override
    public Long call(String request) throws UserCodeExecutionException {
      return client.getLong(request);
    }
  }

  private static class RedisEnqueuer<T> extends RedisSetupTeardown implements Caller<T, Void> {
    private final String key;
    private final Coder<T> coder;

    private RedisEnqueuer(URI uri, String key, Coder<T> coder) {
      super(new RedisClient(uri));
      this.key = key;
      this.coder = coder;
    }

    @Override
    public Void call(T request) throws UserCodeExecutionException {
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

  private static class RedisDequeuer<T> extends RedisSetupTeardown implements Caller<Instant, T> {

    private final Coder<T> coder;
    private final String key;

    private RedisDequeuer(URI uri, Coder<T> coder, String key) {
      super(new RedisClient(uri));
      this.coder = coder;
      this.key = key;
    }

    @Override
    public T call(Instant request) throws UserCodeExecutionException {
      byte[] bytes = client.lpop(key);
      try {
        return checkStateNotNull(coder.decode(ByteSource.wrap(bytes).openStream()));

      } catch (IOException e) {
        throw new UserCodeExecutionException(e);
      }
    }
  }

  private static class RedisDecrementer extends RedisSetupTeardown
      implements Caller<Instant, Long> {

    private final String key;

    private RedisDecrementer(URI uri, String key) {
      super(new RedisClient(uri));
      this.key = key;
    }

    @Override
    public Long call(Instant request) throws UserCodeExecutionException {
      return client.decr(key);
    }
  }

  private static class RedisRefresher extends RedisSetupTeardown implements Caller<Instant, Void> {
    private final Quota quota;
    private final String key;

    private RedisRefresher(URI uri, Quota quota, String key) {
      super(new RedisClient(uri));
      this.quota = quota;
      this.key = key;
    }

    @Override
    public Void call(Instant request) throws UserCodeExecutionException {
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
