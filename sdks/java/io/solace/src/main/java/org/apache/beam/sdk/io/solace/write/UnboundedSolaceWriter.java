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
package org.apache.beam.sdk.io.solace.write;

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This DoFn encapsulates common code used both for the {@link UnboundedBatchedSolaceWriter} and
 * {@link UnboundedStreamingSolaceWriter}.
 */
@Internal
public abstract class UnboundedSolaceWriter
    extends DoFn<KV<Integer, Iterable<Solace.Record>>, Solace.PublishResult> {

  private final Distribution latencyPublish =
      Metrics.distribution(SolaceIO.Write.class, "latency_publish_ms");

  private final Distribution latencyErrors =
      Metrics.distribution(SolaceIO.Write.class, "latency_failed_ms");

  private final SerializableFunction<Solace.Record, Destination> destinationFn;

  private final SessionServiceFactory sessionServiceFactory;
  private final DeliveryMode deliveryMode;
  private final SubmissionMode submissionMode;
  private final int producersMapCardinality;
  private final boolean publishLatencyMetrics;
  private static final AtomicInteger bundleProducerIndexCounter = new AtomicInteger();
  private int currentBundleProducerIndex = 0;

  private @Nullable Instant bundleTimestamp;
  @Nullable ScheduledExecutorService scheduledExecutorService;
  final Duration maxWaitTimeForPublishResponses;

  final UUID writerTransformUuid = UUID.randomUUID();

  public UnboundedSolaceWriter(
      SerializableFunction<Record, Destination> destinationFn,
      SessionServiceFactory sessionServiceFactory,
      DeliveryMode deliveryMode,
      SubmissionMode submissionMode,
      int producersMapCardinality,
      boolean publishLatencyMetrics, Duration maxWaitTimeForPublishResponses) {
    this.destinationFn = destinationFn;
    this.sessionServiceFactory = sessionServiceFactory;
    // Make sure that we set the submission mode now that we know which mode has been set by the
    // user.
    this.sessionServiceFactory.setSubmissionMode(submissionMode);
    this.deliveryMode = deliveryMode;
    this.submissionMode = submissionMode;
    this.producersMapCardinality = producersMapCardinality;
    this.publishLatencyMetrics = publishLatencyMetrics;
    this.maxWaitTimeForPublishResponses = maxWaitTimeForPublishResponses;
  }

  @Teardown
  public void teardown() {
    SolaceWriteSessionsHandler.disconnectFromSolace(
        sessionServiceFactory, producersMapCardinality, writerTransformUuid);
  }

  public void updateProducerIndex() {
    currentBundleProducerIndex =
        bundleProducerIndexCounter.getAndIncrement() % producersMapCardinality;
  }

  @StartBundle
  public void startBundle(PipelineOptions pipelineOptions) {
    // Pick a producer at random for this bundle, reuse for the whole bundle
    updateProducerIndex();
      scheduledExecutorService = pipelineOptions.as(ExecutorOptions.class)
          .getScheduledExecutorService();

  }

  public SessionService solaceSessionServiceWithProducer() {
    return SolaceWriteSessionsHandler.getSessionServiceWithProducer(
        currentBundleProducerIndex, sessionServiceFactory, writerTransformUuid);
  }

  // todo we need this in some form
  // public void publishResults(BeamContextWrapper context) {
  //   long sumPublish = 0;
  //   long countPublish = 0;
  //   long minPublish = Long.MAX_VALUE;
  //   long maxPublish = 0;
  //
  //   long sumFailed = 0;
  //   long countFailed = 0;
  //   long minFailed = Long.MAX_VALUE;
  //   long maxFailed = 0;
  //
  //   Map<String, SettableFuture<PublishResult>> publishResultsQueue =
  //       solaceSessionServiceWithProducer().getPublishedResultsQueue();
  //   Solace.PublishResult result = publishResultsQueue.get("");
  //
  //   if (result != null) {
  //     if (getCurrentBundleTimestamp() == null) {
  //       setCurrentBundleTimestamp(Instant.now());
  //     }
  //   }
  //
  //   while (result != null) {
  //     Long latency = result.getLatencyNanos();
  //
  //     if (latency == null && shouldPublishLatencyMetrics()) {
  //       LOG.error(
  //           "SolaceIO.Write: Latency is null but user asked for latency metrics."
  //               + " This may be a bug.");
  //     }
  //
  //     if (latency != null) {
  //       if (result.getPublished()) {
  //         sumPublish += latency;
  //         countPublish++;
  //         minPublish = Math.min(minPublish, latency);
  //         maxPublish = Math.max(maxPublish, latency);
  //       } else {
  //         sumFailed += latency;
  //         countFailed++;
  //         minFailed = Math.min(minFailed, latency);
  //         maxFailed = Math.max(maxFailed, latency);
  //       }
  //     }
  //     if (result.getPublished()) {
  //       context.output(
  //           SUCCESSFUL_PUBLISH_TAG, result, getCurrentBundleTimestamp(), GlobalWindow.INSTANCE);
  //     } else {
  //       try {
  //         BadRecord b =
  //             BadRecord.fromExceptionInformation(
  //                 result,
  //                 null,
  //                 null,
  //                 Optional.ofNullable(result.getError()).orElse("SolaceIO.Write: unknown error."));
  //         context.output(FAILED_PUBLISH_TAG, b, getCurrentBundleTimestamp(), GlobalWindow.INSTANCE);
  //       } catch (IOException e) {
  //         // ignore, the exception is thrown when the exception argument in the
  //         // `BadRecord.fromExceptionInformation` is not null.
  //       }
  //     }
  //
  //     result = publishResultsQueue.poll();
  //   }
  //
  //   if (shouldPublishLatencyMetrics()) {
  //     // Report all latency value in milliseconds
  //     if (countPublish > 0) {
  //       getPublishLatencyMetric()
  //           .update(
  //               TimeUnit.NANOSECONDS.toMillis(sumPublish),
  //               countPublish,
  //               TimeUnit.NANOSECONDS.toMillis(minPublish),
  //               TimeUnit.NANOSECONDS.toMillis(maxPublish));
  //     }
  //
  //     if (countFailed > 0) {
  //       getFailedLatencyMetric()
  //           .update(
  //               TimeUnit.NANOSECONDS.toMillis(sumFailed),
  //               countFailed,
  //               TimeUnit.NANOSECONDS.toMillis(minFailed),
  //               TimeUnit.NANOSECONDS.toMillis(maxFailed));
  //     }
  //   }
  // }

  public int getProducersMapCardinality() {
    return producersMapCardinality;
  }

  public Distribution getPublishLatencyMetric() {
    return latencyPublish;
  }

  public Distribution getFailedLatencyMetric() {
    return latencyErrors;
  }

  public boolean shouldPublishLatencyMetrics() {
    return publishLatencyMetrics;
  }

  public SerializableFunction<Solace.Record, Destination> getDestinationFn() {
    return destinationFn;
  }

  public DeliveryMode getDeliveryMode() {
    return deliveryMode;
  }

  public SubmissionMode getSubmissionMode() {
    return submissionMode;
  }

  public @Nullable Instant getCurrentBundleTimestamp() {
    return bundleTimestamp;
  }

  public void setCurrentBundleTimestamp(Instant bundleTimestamp) {
    if (this.bundleTimestamp == null || bundleTimestamp.isBefore(this.bundleTimestamp)) {
      this.bundleTimestamp = bundleTimestamp;
    }
  }

  /**
   * Since we need to publish from on timer methods and finish bundle methods, we need a consistent
   * way to handle both WindowedContext and FinishBundleContext.
   */
  static class BeamContextWrapper {
    private @Nullable WindowedContext windowedContext;
    private @Nullable FinishBundleContext finishBundleContext;

    private BeamContextWrapper() {}

    public static BeamContextWrapper of(WindowedContext windowedContext) {
      BeamContextWrapper beamContextWrapper = new BeamContextWrapper();
      beamContextWrapper.windowedContext = windowedContext;
      return beamContextWrapper;
    }

    public static BeamContextWrapper of(FinishBundleContext finishBundleContext) {
      BeamContextWrapper beamContextWrapper = new BeamContextWrapper();
      beamContextWrapper.finishBundleContext = finishBundleContext;
      return beamContextWrapper;
    }

    public <T> void output(
        TupleTag<T> tag,
        T output,
        @Nullable Instant timestamp, // Not required for windowed context
        @Nullable BoundedWindow window) { // Not required for windowed context
      if (windowedContext != null) {
        windowedContext.output(tag, output);
      } else if (finishBundleContext != null) {
        if (timestamp == null) {
          throw new IllegalStateException(
              "SolaceIO.Write.UnboundedSolaceWriter.Context: Timestamp is required for a"
                  + " FinishBundleContext.");
        }
        if (window == null) {
          throw new IllegalStateException(
              "SolaceIO.Write.UnboundedSolaceWriter.Context: BoundedWindow is required for a"
                  + " FinishBundleContext.");
        }
        finishBundleContext.output(tag, output, timestamp, window);
      } else {
        throw new IllegalStateException(
            "SolaceIO.Write.UnboundedSolaceWriter.Context: No context provided");
      }
    }
  }
}
