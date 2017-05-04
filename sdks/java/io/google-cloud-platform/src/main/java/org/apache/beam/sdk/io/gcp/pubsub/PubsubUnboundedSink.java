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

package org.apache.beam.sdk.io.gcp.pubsub;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

/**
 * A PTransform which streams messages to Pubsub.
 * <ul>
 * <li>The underlying implementation is just a {@link GroupByKey} followed by a {@link ParDo} which
 * publishes as a side effect. (In the future we want to design and switch to a custom
 * {@code UnboundedSink} implementation so as to gain access to system watermark and
 * end-of-pipeline cleanup.)
 * <li>We try to send messages in batches while also limiting send latency.
 * <li>No stats are logged. Rather some counters are used to keep track of elements and batches.
 * <li>Though some background threads are used by the underlying netty system all actual Pubsub
 * calls are blocking. We rely on the underlying runner to allow multiple {@link DoFn} instances
 * to execute concurrently and hide latency.
 * <li>A failed bundle will cause messages to be resent. Thus we rely on the Pubsub consumer
 * to dedup messages.
 * </ul>
 */
public class PubsubUnboundedSink extends PTransform<PCollection<PubsubMessage>, PDone> {
  /**
   * Default maximum number of messages per publish.
   */
  private static final int DEFAULT_PUBLISH_BATCH_SIZE = 1000;

  /**
   * Default maximum size of a publish batch, in bytes.
   */
  private static final int DEFAULT_PUBLISH_BATCH_BYTES = 400000;

  /**
   * Default longest delay between receiving a message and pushing it to Pubsub.
   */
  private static final Duration DEFAULT_MAX_LATENCY = Duration.standardSeconds(2);

  /**
   * Coder for conveying outgoing messages between internal stages.
   */
  private static class OutgoingMessageCoder extends CustomCoder<OutgoingMessage> {
    private static final NullableCoder<String> RECORD_ID_CODER =
        NullableCoder.of(StringUtf8Coder.of());
    private static final NullableCoder<Map<String, String>> ATTRIBUTES_CODER =
            NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    @Override
    public void encode(
        OutgoingMessage value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      ByteArrayCoder.of().encode(value.elementBytes, outStream, context.nested());
      ATTRIBUTES_CODER.encode(value.attributes, outStream, context.nested());
      BigEndianLongCoder.of().encode(value.timestampMsSinceEpoch, outStream, context.nested());
      RECORD_ID_CODER.encode(value.recordId, outStream, context.nested());
    }

    @Override
    public OutgoingMessage decode(
        InputStream inStream, Context context) throws CoderException, IOException {
      byte[] elementBytes = ByteArrayCoder.of().decode(inStream, context.nested());
      Map<String, String> attributes = ATTRIBUTES_CODER.decode(inStream, context.nested());
      long timestampMsSinceEpoch = BigEndianLongCoder.of().decode(inStream, context.nested());
      @Nullable String recordId = RECORD_ID_CODER.decode(inStream, context.nested());
      return new OutgoingMessage(elementBytes, attributes, timestampMsSinceEpoch, recordId);
    }
  }

  @VisibleForTesting
  static final Coder<OutgoingMessage> CODER = new OutgoingMessageCoder();

  // ================================================================================
  // RecordIdMethod
  // ================================================================================

  /**
   * Specify how record ids are to be generated.
   */
  @VisibleForTesting
  enum RecordIdMethod {
    /** Leave null. */
    NONE,
    /** Generate randomly. */
    RANDOM,
    /** Generate deterministically. For testing only. */
    DETERMINISTIC
  }

  // ================================================================================
  // ShardFn
  // ================================================================================

  /**
   * Convert elements to messages and shard them.
   */
  private static class ShardFn extends DoFn<PubsubMessage, KV<Integer, OutgoingMessage>> {
    private final Counter elementCounter = Metrics.counter(ShardFn.class, "elements");
    private final int numShards;
    private final RecordIdMethod recordIdMethod;

    ShardFn(int numShards, RecordIdMethod recordIdMethod) {
      this.numShards = numShards;
      this.recordIdMethod = recordIdMethod;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      elementCounter.inc();
      PubsubMessage message = c.element();
      byte[] elementBytes = message.getPayload();
      Map<String, String> attributes = message.getAttributeMap();

      long timestampMsSinceEpoch = c.timestamp().getMillis();
      @Nullable String recordId = null;
      switch (recordIdMethod) {
        case NONE:
          break;
        case DETERMINISTIC:
          recordId = Hashing.murmur3_128().hashBytes(elementBytes).toString();
          break;
        case RANDOM:
          // Since these elements go through a GroupByKey, any  failures while sending to
          // Pubsub will be retried without falling back and generating a new record id.
          // Thus even though we may send the same message to Pubsub twice, it is guaranteed
          // to have the same record id.
          recordId = UUID.randomUUID().toString();
          break;
      }
      c.output(KV.of(ThreadLocalRandom.current().nextInt(numShards),
                     new OutgoingMessage(elementBytes, attributes, timestampMsSinceEpoch,
                             recordId)));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("numShards", numShards));
    }
  }

  // ================================================================================
  // WriterFn
  // ================================================================================

  /**
   * Publish messages to Pubsub in batches.
   */
  private static class WriterFn
      extends DoFn<KV<Integer, Iterable<OutgoingMessage>>, Void> {
    private final PubsubClientFactory pubsubFactory;
    private final ValueProvider<TopicPath> topic;
    private final String timestampAttribute;
    private final String idAttribute;
    private final int publishBatchSize;
    private final int publishBatchBytes;

    /**
     * Client on which to talk to Pubsub. Null until created by {@link #startBundle}.
     */
    @Nullable
    private transient PubsubClient pubsubClient;

    private final Counter batchCounter = Metrics.counter(WriterFn.class, "batches");
    private final Counter elementCounter = SinkMetrics.elementsWritten();
    private final Counter byteCounter = SinkMetrics.bytesWritten();

    WriterFn(
        PubsubClientFactory pubsubFactory,
        ValueProvider<TopicPath> topic,
        String timestampAttribute,
        String idAttribute,
        int publishBatchSize,
        int publishBatchBytes) {
      this.pubsubFactory = pubsubFactory;
      this.topic = topic;
      this.timestampAttribute = timestampAttribute;
      this.idAttribute = idAttribute;
      this.publishBatchSize = publishBatchSize;
      this.publishBatchBytes = publishBatchBytes;
    }

    /**
     * BLOCKING
     * Send {@code messages} as a batch to Pubsub.
     */
    private void publishBatch(List<OutgoingMessage> messages, int bytes)
        throws IOException {
      int n = pubsubClient.publish(topic.get(), messages);
      checkState(n == messages.size(), "Attempted to publish %s messages but %s were successful",
                 messages.size(), n);
      batchCounter.inc();
      elementCounter.inc(messages.size());
      byteCounter.inc(bytes);
    }

    @StartBundle
    public void startBundle(Context c) throws Exception {
      checkState(pubsubClient == null, "startBundle invoked without prior finishBundle");
      pubsubClient = pubsubFactory.newClient(timestampAttribute, idAttribute,
                                             c.getPipelineOptions().as(PubsubOptions.class));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      List<OutgoingMessage> pubsubMessages = new ArrayList<>(publishBatchSize);
      int bytes = 0;
      for (OutgoingMessage message : c.element().getValue()) {
        if (!pubsubMessages.isEmpty()
            && bytes + message.elementBytes.length > publishBatchBytes) {
          // Break large (in bytes) batches into smaller.
          // (We've already broken by batch size using the trigger below, though that may
          // run slightly over the actual PUBLISH_BATCH_SIZE. We'll consider that ok since
          // the hard limit from Pubsub is by bytes rather than number of messages.)
          // BLOCKS until published.
          publishBatch(pubsubMessages, bytes);
          pubsubMessages.clear();
          bytes = 0;
        }
        pubsubMessages.add(message);
        bytes += message.elementBytes.length;
      }
      if (!pubsubMessages.isEmpty()) {
        // BLOCKS until published.
        publishBatch(pubsubMessages, bytes);
      }
    }

    @FinishBundle
    public void finishBundle(Context c) throws Exception {
      pubsubClient.close();
      pubsubClient = null;
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
        String topicString =
            topic == null ? null
            : topic.isAccessible() ? topic.get().getPath()
            : topic.toString();
      builder.add(DisplayData.item("topic", topicString));
      builder.add(DisplayData.item("transport", pubsubFactory.getKind()));
      builder.addIfNotNull(DisplayData.item("timestampAttribute", timestampAttribute));
      builder.addIfNotNull(DisplayData.item("idAttribute", idAttribute));
    }
  }

  // ================================================================================
  // PubsubUnboundedSink
  // ================================================================================

  /**
   * Which factory to use for creating Pubsub transport.
   */
  private final PubsubClientFactory pubsubFactory;

  /**
   * Pubsub topic to publish to.
   */
  private final ValueProvider<TopicPath> topic;

  /**
   * Pubsub metadata field holding timestamp of each element, or {@literal null} if should use
   * Pubsub message publish timestamp instead.
   */
  @Nullable
  private final String timestampAttribute;

  /**
   * Pubsub metadata field holding id for each element, or {@literal null} if need to generate
   * a unique id ourselves.
   */
  @Nullable
  private final String idAttribute;

  /**
   * Number of 'shards' to use so that latency in Pubsub publish can be hidden. Generally this
   * should be a small multiple of the number of available cores. Too smoll a number results
   * in too much time lost to blocking Pubsub calls. To large a number results in too many
   * single-element batches being sent to Pubsub with high per-batch overhead.
   */
  private final int numShards;

  /**
   * Maximum number of messages per publish.
   */
  private final int publishBatchSize;

  /**
   * Maximum size of a publish batch, in bytes.
   */
  private final int publishBatchBytes;

  /**
   * Longest delay between receiving a message and pushing it to Pubsub.
   */
  private final Duration maxLatency;

  /**
   * How record ids should be generated for each record (if {@link #idAttribute} is non-{@literal
   * null}).
   */
  private final RecordIdMethod recordIdMethod;

  @VisibleForTesting
  PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      ValueProvider<TopicPath> topic,
      String timestampAttribute,
      String idAttribute,
      int numShards,
      int publishBatchSize,
      int publishBatchBytes,
      Duration maxLatency,
      RecordIdMethod recordIdMethod) {
    this.pubsubFactory = pubsubFactory;
    this.topic = topic;
    this.timestampAttribute = timestampAttribute;
    this.idAttribute = idAttribute;
    this.numShards = numShards;
    this.publishBatchSize = publishBatchSize;
    this.publishBatchBytes = publishBatchBytes;
    this.maxLatency = maxLatency;
    this.recordIdMethod = idAttribute == null ? RecordIdMethod.NONE : recordIdMethod;
  }

  public PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      ValueProvider<TopicPath> topic,
      String timestampAttribute,
      String idAttribute,
      int numShards) {
    this(pubsubFactory, topic, timestampAttribute, idAttribute, numShards,
         DEFAULT_PUBLISH_BATCH_SIZE, DEFAULT_PUBLISH_BATCH_BYTES, DEFAULT_MAX_LATENCY,
         RecordIdMethod.RANDOM);
  }

  /**
   * Get the topic being written to.
   */
  public TopicPath getTopic() {
    return topic.get();
  }

  /**
   * Get the {@link ValueProvider} for the topic being written to.
   */
  public ValueProvider<TopicPath> getTopicProvider() {
    return topic;
  }

  /**
   * Get the timestamp attribute.
   */
  @Nullable
  public String getTimestampAttribute() {
    return timestampAttribute;
  }

  /**
   * Get the id attribute.
   */
  @Nullable
  public String getIdAttribute() {
    return idAttribute;
  }

  @Override
  public PDone expand(PCollection<PubsubMessage> input) {
    input
        .apply(
            "PubsubUnboundedSink.Window",
            Window.<PubsubMessage>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterFirst.of(
                            AfterPane.elementCountAtLeast(publishBatchSize),
                            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(maxLatency))))
                .discardingFiredPanes())
        .apply("PubsubUnboundedSink.Shard", ParDo.of(new ShardFn(numShards, recordIdMethod)))
        .setCoder(KvCoder.of(VarIntCoder.of(), CODER))
        .apply(GroupByKey.<Integer, OutgoingMessage>create())
        .apply(
            "PubsubUnboundedSink.Writer",
            ParDo.of(
                new WriterFn(
                    pubsubFactory,
                    topic,
                    timestampAttribute,
                    idAttribute,
                    publishBatchSize,
                    publishBatchBytes)));
    return PDone.in(input.getPipeline());
  }
}
