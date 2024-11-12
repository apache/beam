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

import static org.apache.beam.sdk.io.solace.SolaceIO.Write.FAILED_PUBLISH_TAG;
import static org.apache.beam.sdk.io.solace.SolaceIO.Write.SUCCESSFUL_PUBLISH_TAG;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSendMultipleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public final class UnboundedSolaceWriter {

  /**
   * This DoFn encapsulates common code used both for the {@link UnboundedBatchedSolaceWriter} and
   * {@link UnboundedStreamingSolaceWriter}.
   */
  abstract static class AbstractWriterDoFn
      extends DoFn<KV<Integer, Solace.Record>, Solace.PublishResult> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWriterDoFn.class);

    // This is the batch limit supported by the send multiple JCSMP API method.
    static final int SOLACE_BATCH_LIMIT = 50;
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
    private int currentBundleProducerIndex = 0;

    private final List<Solace.Record> batchToEmit;

    private @Nullable Instant bundleTimestamp;
    private @Nullable BoundedWindow bundleWindow;

    public AbstractWriterDoFn(
        SerializableFunction<Solace.Record, Destination> destinationFn,
        SessionServiceFactory sessionServiceFactory,
        DeliveryMode deliveryMode,
        SubmissionMode submissionMode,
        int producersMapCardinality,
        boolean publishLatencyMetrics) {
      this.destinationFn = destinationFn;
      this.sessionServiceFactory = sessionServiceFactory;
      this.deliveryMode = deliveryMode;
      this.submissionMode = submissionMode;
      this.producersMapCardinality = producersMapCardinality;
      this.publishLatencyMetrics = publishLatencyMetrics;
      this.batchToEmit = new ArrayList<>();
    }

    @Teardown
    public void teardown() {
      SolaceWriteSessionsHandler.disconnectFromSolace(
          sessionServiceFactory, producersMapCardinality);
    }

    public void updateProducerIndex() {
      currentBundleProducerIndex = (int) (Math.random() * producersMapCardinality);
    }

    @StartBundle
    public void startBundle() {
      // Pick a producer at random for this bundle, reuse for the whole bundle
      updateProducerIndex();
      batchToEmit.clear();
    }

    public SessionService solaceSessionService() {
      return SolaceWriteSessionsHandler.getSessionService(
          currentBundleProducerIndex, sessionServiceFactory);
    }

    public void publishResults(BeamContextWrapper context) {
      long sumPublish = 0;
      long countPublish = 0;
      long minPublish = Long.MAX_VALUE;
      long maxPublish = 0;

      long sumFailed = 0;
      long countFailed = 0;
      long minFailed = Long.MAX_VALUE;
      long maxFailed = 0;

      Solace.PublishResult result = PublishResultsReceiver.pollResults();

      if (result != null) {
        if (getCurrentBundleTimestamp() == null) {
          setCurrentBundleTimestamp(Instant.now());
        }

        if (getCurrentBundleWindow() == null) {
          setCurrentBundleWindow(GlobalWindow.INSTANCE);
        }
      }

      while (result != null) {
        Long latency = result.getLatencyMilliseconds();

        if (latency == null && shouldPublishLatencyMetrics()) {
          LOG.error(
              "SolaceIO.Write: Latency is null but user asked for latency metrics."
                  + " This may be a bug.");
        }

        if (latency != null) {
          if (result.getPublished()) {
            sumPublish += latency;
            countPublish++;
            minPublish = Math.min(minPublish, latency);
            maxPublish = Math.max(maxPublish, latency);
          } else {
            sumFailed += latency;
            countFailed++;
            minFailed = Math.min(minFailed, latency);
            maxFailed = Math.max(maxFailed, latency);
          }
        }

        if (result.getPublished()) {
          context.output(
              SUCCESSFUL_PUBLISH_TAG,
              result,
              getCurrentBundleTimestamp(),
              getCurrentBundleWindow());
        } else {
          context.output(
              FAILED_PUBLISH_TAG, result, getCurrentBundleTimestamp(), getCurrentBundleWindow());
        }

        result = PublishResultsReceiver.pollResults();
      }

      if (shouldPublishLatencyMetrics()) {
        if (countPublish > 0) {
          getPublishLatencyMetric().update(sumPublish, countPublish, minPublish, maxPublish);
        }

        if (countFailed > 0) {
          getFailedLatencyMetric().update(sumFailed, countFailed, minFailed, maxFailed);
        }
      }
    }

    public BytesXMLMessage createSingleMessage(
        Solace.Record record, boolean useCorrelationKeyLatency) {
      JCSMPFactory jcsmpFactory = JCSMPFactory.onlyInstance();
      BytesXMLMessage msg = jcsmpFactory.createBytesXMLMessage();
      byte[] payload = record.getPayload();
      msg.writeBytes(payload);

      Long senderTimestamp = record.getSenderTimestamp();
      if (senderTimestamp == null) {
        LOG.error(
            "SolaceIO.Write: Record with id {} has no sender timestamp. Using current"
                + " worker clock as timestamp.",
            record.getMessageId());
        senderTimestamp = System.currentTimeMillis();
      }
      msg.setSenderTimestamp(senderTimestamp);
      msg.setDeliveryMode(getDeliveryMode());
      if (useCorrelationKeyLatency) {
        Solace.CorrelationKey key =
            Solace.CorrelationKey.builder()
                .setMessageId(record.getMessageId())
                .setPublishMonotonicMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()))
                .build();
        msg.setCorrelationKey(key);
      } else {
        // Use only a string as correlation key
        msg.setCorrelationKey(record.getMessageId());
      }
      msg.setApplicationMessageId(record.getMessageId());
      return msg;
    }

    public JCSMPSendMultipleEntry[] createMessagesArray(
        Iterable<Solace.Record> records, boolean useCorrelationKeyLatency) {
      // Solace batch publishing only supports 50 elements max, so it is safe to convert to
      // list here
      ArrayList<Solace.Record> recordsList = Lists.newArrayList(records);
      if (recordsList.size() > SOLACE_BATCH_LIMIT) {
        LOG.error(
            "SolaceIO.Write: Trying to create a batch of {}, but Solace supports a"
                + " maximum of {}. The batch will likely be rejected by Solace.",
            recordsList.size(),
            SOLACE_BATCH_LIMIT);
      }

      JCSMPSendMultipleEntry[] entries = new JCSMPSendMultipleEntry[recordsList.size()];
      for (int i = 0; i < recordsList.size(); i++) {
        Solace.Record record = recordsList.get(i);
        JCSMPSendMultipleEntry entry =
            JCSMPFactory.onlyInstance()
                .createSendMultipleEntry(
                    createSingleMessage(record, useCorrelationKeyLatency),
                    getDestinationFn().apply(record));
        entries[i] = entry;
      }

      return entries;
    }

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

    public SubmissionMode getDispatchMode() {
      return submissionMode;
    }

    public void addToCurrentBundle(Solace.Record record) {
      batchToEmit.add(record);
    }

    public List<Solace.Record> getCurrentBundle() {
      return batchToEmit;
    }

    public @Nullable Instant getCurrentBundleTimestamp() {
      return bundleTimestamp;
    }

    public @Nullable BoundedWindow getCurrentBundleWindow() {
      return bundleWindow;
    }

    public void setCurrentBundleTimestamp(Instant bundleTimestamp) {
      if (this.bundleTimestamp == null || bundleTimestamp.isBefore(this.bundleTimestamp)) {
        this.bundleTimestamp = bundleTimestamp;
      }
    }

    public void setCurrentBundleWindow(BoundedWindow bundleWindow) {
      this.bundleWindow = bundleWindow;
    }

    /**
     * Since we need to publish from on timer methods and finish bundle methods, we need a
     * consistent way to handle both WindowedContext and FinishBundleContext.
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

      public void output(
          TupleTag<Solace.PublishResult> tag,
          Solace.PublishResult output,
          @Nullable Instant timestamp, // Not required for windowed context
          @Nullable BoundedWindow window) { // Not required for windowed context
        if (windowedContext != null) {
          windowedContext.output(tag, output);
        } else if (finishBundleContext != null) {
          if (timestamp == null) {
            throw new IllegalStateException(
                "SolaceIO.Write.UnboundedSolaceWriter.Context: Timestamp is required for a FinishBundleContext.");
          }
          if (window == null) {
            throw new IllegalStateException(
                "SolaceIO.Write.UnboundedSolaceWriter.Context: BoundedWindow is required for a FinishBundleContext.");
          }
          finishBundleContext.output(tag, output, timestamp, window);
        } else {
          throw new IllegalStateException(
              "SolaceIO.Write.UnboundedSolaceWriter.Context: No context provided");
        }
      }
    }
  }

  /**
   * This class a pseudo-key with a given cardinality. The downstream steps will use state & timers
   * to distribute the data and control for the number of parallel workers used for writing.
   */
  @Internal
  public static class AddShardKeyDoFn extends DoFn<Solace.Record, KV<Integer, Solace.Record>> {
    private final int shardCount;
    private int shardKey;

    public AddShardKeyDoFn(int shardCount) {
      this.shardCount = shardCount;
      shardKey = -1;
    }

    @ProcessElement
    public void processElement(
        @Element Solace.Record record, OutputReceiver<KV<Integer, Solace.Record>> c) {
      shardKey = (shardKey + 1) % shardCount;
      c.output(KV.of(shardKey, record));
    }
  }

  /**
   * This class just transforms to PublishResult to be able to capture the windowing with the right
   * strategy. The output is not used for anything else.
   */
  @Internal
  public static class RecordToPublishResultDoFn extends DoFn<Solace.Record, Solace.PublishResult> {
    @ProcessElement
    public void processElement(
        @Element Solace.Record record, OutputReceiver<Solace.PublishResult> receiver) {
      Solace.PublishResult result =
          Solace.PublishResult.builder()
              .setPublished(true)
              .setMessageId(record.getMessageId())
              .setLatencyMilliseconds(0L)
              .build();
      receiver.output(result);
    }
  }
}
