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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public final class UnboundedBatchedSolaceWriter {
  /**
   * This DoFn is the responsible for writing to Solace in batch mode (holding up any messages), and
   * emit the corresponding output (success or fail; only for persistent messages), so the
   * SolaceIO.Write connector can be composed with other subsequent transforms in the pipeline.
   *
   * <p>The DoFn will create several JCSMP sessions per VM, and the sessions and producers will be
   * reused across different threads (if the number of threads is higher than the number of
   * sessions, which is probably the most common case).
   *
   * <p>The producer uses the JCSMP send multiple mode to publish a batch of messages together with
   * a single API call. The acks from this publication are also processed in batch, and returned as
   * the output of the DoFn.
   *
   * <p>The batch size is 50, and this is currently the maximum value supported by Solace.
   *
   * <p>There are no acks if the delivery mode is set to DIRECT.
   *
   * <p>This writer DoFn offers higher throughput than {@link
   * UnboundedStreamingSolaceWriter.WriterDoFn} but also higher latency.
   */
  @Internal
  public static class WriterDoFn extends UnboundedSolaceWriter.AbstractWriterDoFn {

    private static final Logger LOG = LoggerFactory.getLogger(WriterDoFn.class);

    private static final int ACKS_FLUSHING_INTERVAL_SECS = 10;

    private final Counter sentToBroker =
        Metrics.counter(UnboundedBatchedSolaceWriter.class, "msgs_sent_to_broker");

    private final Counter batchesRejectedByBroker =
        Metrics.counter(UnboundedBatchedSolaceWriter.class, "batches_rejected");

    // State variables are never explicitly "used"
    @SuppressWarnings("UnusedVariable")
    @StateId("processing_key")
    private final StateSpec<ValueState<Integer>> processingKeySpec = StateSpecs.value();

    @SuppressWarnings("UnusedVariable")
    @TimerId("bundle_flusher")
    private final TimerSpec bundleFlusherTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    public WriterDoFn(
        SerializableFunction<Solace.Record, Destination> destinationFn,
        SessionServiceFactory sessionServiceFactory,
        DeliveryMode deliveryMode,
        SubmissionMode submissionMode,
        int producersMapCardinality,
        boolean publishLatencyMetrics) {
      super(
          destinationFn,
          sessionServiceFactory,
          deliveryMode,
          submissionMode,
          producersMapCardinality,
          publishLatencyMetrics);
    }

    // The state variable is here just to force a shuffling with a certain cardinality
    @ProcessElement
    public void processElement(
        @Element KV<Integer, Solace.Record> element,
        @StateId("processing_key") ValueState<Integer> ignoredProcessingKey,
        @TimerId("bundle_flusher") Timer bundleFlusherTimer,
        @Timestamp Instant timestamp,
        BoundedWindow window) {

      setCurrentBundleTimestamp(timestamp);
      setCurrentBundleWindow(window);

      Solace.Record record = element.getValue();

      if (record == null) {
        LOG.error(
            "SolaceIO.Write: Found null record with key {}. Ignoring record.", element.getKey());
      } else {
        addToCurrentBundle(record);
        // Extend timer for bundle flushing
        bundleFlusherTimer
            .offset(Duration.standardSeconds(ACKS_FLUSHING_INTERVAL_SECS))
            .setRelative();
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      // Take messages in groups of 50 (if there are enough messages)
      List<Solace.Record> currentBundle = getCurrentBundle();
      for (int i = 0; i < currentBundle.size(); i += SOLACE_BATCH_LIMIT) {
        int toIndex = Math.min(i + SOLACE_BATCH_LIMIT, currentBundle.size());
        List<Solace.Record> batch = currentBundle.subList(i, toIndex);
        if (batch.isEmpty()) {
          continue;
        }
        publishBatch(batch);
      }
      getCurrentBundle().clear();

      publishResults(BeamContextWrapper.of(context));
    }

    @OnTimer("bundle_flusher")
    public void flushBundle(OnTimerContext context) {
      publishResults(BeamContextWrapper.of(context));
    }

    private void publishBatch(List<Solace.Record> records) {
      try {
        int entriesPublished =
            solaceSessionService()
                .getProducer()
                .publishBatch(
                    records, shouldPublishLatencyMetrics(), getDestinationFn(), getDeliveryMode());
        sentToBroker.inc(entriesPublished);
      } catch (Exception e) {
        batchesRejectedByBroker.inc();
        Solace.PublishResult errorPublish =
            Solace.PublishResult.builder()
                .setPublished(false)
                .setMessageId(String.format("BATCH_OF_%d_ENTRIES", records.size()))
                .setError(
                    String.format(
                        "Batch could not be published after several" + " retries. Error: %s",
                        e.getMessage()))
                .setLatencyMilliseconds(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()))
                .build();
        PublishResultsReceiver.addResult(errorPublish);
      }
    }
  }
}
