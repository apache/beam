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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.MapValues;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A PTransform which streams messages to Pubsub.
 *
 * <ul>
 *   <li>The underlying implementation is just a {@link GroupByKey} followed by a {@link ParDo}
 *       which publishes as a side effect. (In the future we want to design and switch to a custom
 *       {@code UnboundedSink} implementation so as to gain access to system watermark and
 *       end-of-pipeline cleanup.)
 *   <li>We try to send messages in batches while also limiting send latency.
 *   <li>No stats are logged. Rather some counters are used to keep track of elements and batches.
 *   <li>Though some background threads are used by the underlying netty system all actual Pubsub
 *       calls are blocking. We rely on the underlying runner to allow multiple {@link DoFn}
 *       instances to execute concurrently and hide latency.
 *   <li>A failed bundle will cause messages to be resent. Thus we rely on the Pubsub consumer to
 *       dedup messages.
 * </ul>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubUnboundedSink extends PTransform<PCollection<PubsubMessage>, PDone> {
  /** Default maximum number of messages per publish. */
  static final int DEFAULT_PUBLISH_BATCH_SIZE = 1000;

  /** Default maximum size of a publish batch, in bytes. */
  static final int DEFAULT_PUBLISH_BATCH_BYTES = 400000;

  /** Default longest delay between receiving a message and pushing it to Pubsub. */
  private static final Duration DEFAULT_MAX_LATENCY = Duration.standardSeconds(2);

  /** Coder for conveying outgoing messages between internal stages. */
  /** Coder for conveying outgoing messages between internal stages. */
  private static class OutgoingMessageCoder extends AtomicCoder<OutgoingMessage> {
    private static final NullableCoder<String> RECORD_ID_CODER =
        NullableCoder.of(StringUtf8Coder.of());
    private static final NullableCoder<String> TOPIC_CODER = NullableCoder.of(StringUtf8Coder.of());

    @Override
    public void encode(OutgoingMessage value, OutputStream outStream)
        throws CoderException, IOException {
      ProtoCoder.of(com.google.pubsub.v1.PubsubMessage.class).encode(value.getMessage(), outStream);
      BigEndianLongCoder.of().encode(value.getTimestampMsSinceEpoch(), outStream);
      RECORD_ID_CODER.encode(value.recordId(), outStream);
      TOPIC_CODER.encode(value.topic(), outStream);
    }

    @Override
    public OutgoingMessage decode(InputStream inStream) throws CoderException, IOException {
      com.google.pubsub.v1.PubsubMessage message =
          ProtoCoder.of(com.google.pubsub.v1.PubsubMessage.class).decode(inStream);
      long timestampMsSinceEpoch = BigEndianLongCoder.of().decode(inStream);
      @Nullable String recordId = RECORD_ID_CODER.decode(inStream);
      @Nullable String topic = TOPIC_CODER.decode(inStream);
      return OutgoingMessage.of(message, timestampMsSinceEpoch, recordId, topic);
    }
  }

  @VisibleForTesting static final Coder<OutgoingMessage> CODER = new OutgoingMessageCoder();

  // ================================================================================
  // RecordIdMethod
  // ================================================================================

  /** Specify how record ids are to be generated. */
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

  /** Convert elements to messages and shard them. */
  private static class ShardFn<T, K> extends DoFn<T, KV<K, OutgoingMessage>> {
    private final Counter elementCounter = Metrics.counter(ShardFn.class, "elements");
    private final int numShards;
    private final RecordIdMethod recordIdMethod;

    private final SerializableFunction<T, com.google.pubsub.v1.PubsubMessage> toProto;
    private final SerializableFunction<T, @Nullable String> dynamicTopicFn;

    private final SerializableBiFunction<Integer, @Nullable String, K> keyFunction;

    ShardFn(
        int numShards,
        RecordIdMethod recordIdMethod,
        SerializableFunction<T, com.google.pubsub.v1.PubsubMessage> toProto,
        SerializableFunction<T, @Nullable String> dynamicTopicFn,
        SerializableBiFunction<Integer, @Nullable String, K> keyFunction) {
      this.numShards = numShards;
      this.recordIdMethod = recordIdMethod;
      this.toProto = toProto;
      this.dynamicTopicFn = dynamicTopicFn;
      this.keyFunction = keyFunction;
    }

    @ProcessElement
    public void processElement(
        @Element T element, @Timestamp Instant timestamp, OutputReceiver<KV<K, OutgoingMessage>> o)
        throws Exception {
      com.google.pubsub.v1.PubsubMessage message = toProto.apply(element);
      elementCounter.inc();
      byte[] elementBytes = message.getData().toByteArray();

      long timestampMsSinceEpoch = timestamp.getMillis();
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

      @Nullable String topic = dynamicTopicFn.apply(element);
      K key = keyFunction.apply(ThreadLocalRandom.current().nextInt(numShards), topic);
      o.output(KV.of(key, OutgoingMessage.of(message, timestampMsSinceEpoch, recordId, topic)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("numShards", numShards));
    }
  }

  // ================================================================================
  // WriterFn
  // ================================================================================

  /** Publish messages to Pubsub in batches. */
  private static class WriterFn extends DoFn<Iterable<OutgoingMessage>, Void> {
    private final PubsubClientFactory pubsubFactory;
    private final @Nullable ValueProvider<TopicPath> topic;
    private final String timestampAttribute;
    private final String idAttribute;
    private final int publishBatchSize;
    private final int publishBatchBytes;

    private final String pubsubRootUrl;

    /** Client on which to talk to Pubsub. Null until created by {@link #startBundle}. */
    private transient @Nullable PubsubClient pubsubClient;

    private final Counter batchCounter = Metrics.counter(WriterFn.class, "batches");
    private final Counter elementCounter = SinkMetrics.elementsWritten();
    private final Counter byteCounter = SinkMetrics.bytesWritten();

    WriterFn(
        PubsubClientFactory pubsubFactory,
        @Nullable ValueProvider<TopicPath> topic,
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
      this.pubsubRootUrl = null;
    }

    WriterFn(
        PubsubClientFactory pubsubFactory,
        @Nullable ValueProvider<TopicPath> topic,
        String timestampAttribute,
        String idAttribute,
        int publishBatchSize,
        int publishBatchBytes,
        String pubsubRootUrl) {
      this.pubsubFactory = pubsubFactory;
      this.topic = topic;
      this.timestampAttribute = timestampAttribute;
      this.idAttribute = idAttribute;
      this.publishBatchSize = publishBatchSize;
      this.publishBatchBytes = publishBatchBytes;
      this.pubsubRootUrl = pubsubRootUrl;
    }

    /** BLOCKING Send {@code messages} as a batch to Pubsub. */
    private void publishBatch(List<OutgoingMessage> messages, int bytes) throws IOException {
      Preconditions.checkState(!messages.isEmpty());
      TopicPath topicPath;
      if (topic != null) {
        topicPath = topic.get();
      } else {
        // This is the dynamic topic destinations case. Since we first group by topic, we can assume
        // that all messages in the batch have the same topic.
        topicPath =
            PubsubClient.topicPathFromPath(
                org.apache.beam.sdk.util.Preconditions.checkStateNotNull(messages.get(0).topic()));
      }
      int n = pubsubClient.publish(topicPath, messages);
      checkState(
          n == messages.size(),
          "Attempted to publish %s messages but %s were successful",
          messages.size(),
          n);
      batchCounter.inc();
      elementCounter.inc(messages.size());
      byteCounter.inc(bytes);
    }

    @StartBundle
    public void startBundle(StartBundleContext c) throws Exception {
      checkState(pubsubClient == null, "startBundle invoked without prior finishBundle");
      // TODO: Do we really need to recreate the client on every bundle?
      pubsubClient =
          pubsubFactory.newClient(
              timestampAttribute,
              idAttribute,
              c.getPipelineOptions().as(PubsubOptions.class),
              pubsubRootUrl);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      List<OutgoingMessage> pubsubMessages = new ArrayList<>(publishBatchSize);
      int bytes = 0;
      for (OutgoingMessage message : c.element()) {
        if (!pubsubMessages.isEmpty()
            && bytes + message.getMessage().getData().size() > publishBatchBytes) {
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
        bytes += message.getMessage().getData().size();
      }
      if (!pubsubMessages.isEmpty()) {
        // BLOCKS until published.
        publishBatch(pubsubMessages, bytes);
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      pubsubClient.close();
      pubsubClient = null;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("topic", topic));
      builder.add(DisplayData.item("transport", pubsubFactory.getKind()));
      builder.addIfNotNull(DisplayData.item("timestampAttribute", timestampAttribute));
      builder.addIfNotNull(DisplayData.item("idAttribute", idAttribute));
    }
  }

  // ================================================================================
  // PubsubUnboundedSink
  // ================================================================================

  /** Which factory to use for creating Pubsub transport. */
  private final PubsubClientFactory pubsubFactory;

  /**
   * Pubsub topic to publish to. If null, that indicates that the PubsubMessage instead contains the
   * topic.
   */
  private final @Nullable ValueProvider<TopicPath> topic;

  /**
   * Pubsub metadata field holding timestamp of each element, or {@literal null} if should use
   * Pubsub message publish timestamp instead.
   */
  private final @Nullable String timestampAttribute;

  /**
   * Pubsub metadata field holding id for each element, or {@literal null} if need to generate a
   * unique id ourselves.
   */
  private final @Nullable String idAttribute;

  /**
   * Number of 'shards' to use so that latency in Pubsub publish can be hidden. Generally this
   * should be a small multiple of the number of available cores. Too smoll a number results in too
   * much time lost to blocking Pubsub calls. To large a number results in too many single-element
   * batches being sent to Pubsub with high per-batch overhead.
   */
  private final int numShards;

  /** Maximum number of messages per publish. */
  private final int publishBatchSize;

  /** Maximum size of a publish batch, in bytes. */
  private final int publishBatchBytes;

  /** Longest delay between receiving a message and pushing it to Pubsub. */
  private final Duration maxLatency;

  /**
   * How record ids should be generated for each record (if {@link #idAttribute} is non-{@literal
   * null}).
   */
  private final RecordIdMethod recordIdMethod;

  private final String pubsubRootUrl;

  @VisibleForTesting
  PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<TopicPath> topic,
      String timestampAttribute,
      String idAttribute,
      int numShards,
      int publishBatchSize,
      int publishBatchBytes,
      Duration maxLatency,
      RecordIdMethod recordIdMethod,
      String pubsubRootUrl) {
    this.pubsubFactory = pubsubFactory;
    this.topic = topic;
    this.timestampAttribute = timestampAttribute;
    this.idAttribute = idAttribute;
    this.numShards = numShards;
    this.publishBatchSize = publishBatchSize;
    this.publishBatchBytes = publishBatchBytes;
    this.maxLatency = maxLatency;
    this.pubsubRootUrl = pubsubRootUrl;
    this.recordIdMethod = idAttribute == null ? RecordIdMethod.NONE : recordIdMethod;
  }

  public PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      ValueProvider<TopicPath> topic,
      String timestampAttribute,
      String idAttribute,
      int numShards) {
    this(
        pubsubFactory,
        topic,
        timestampAttribute,
        idAttribute,
        numShards,
        DEFAULT_PUBLISH_BATCH_SIZE,
        DEFAULT_PUBLISH_BATCH_BYTES,
        DEFAULT_MAX_LATENCY,
        RecordIdMethod.RANDOM,
        null);
  }

  public PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      ValueProvider<TopicPath> topic,
      String timestampAttribute,
      String idAttribute,
      int numShards,
      String pubsubRootUrl) {
    this(
        pubsubFactory,
        topic,
        timestampAttribute,
        idAttribute,
        numShards,
        DEFAULT_PUBLISH_BATCH_SIZE,
        DEFAULT_PUBLISH_BATCH_BYTES,
        DEFAULT_MAX_LATENCY,
        RecordIdMethod.RANDOM,
        pubsubRootUrl);
  }

  public PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      ValueProvider<TopicPath> topic,
      String timestampAttribute,
      String idAttribute,
      int numShards,
      int publishBatchSize,
      int publishBatchBytes) {
    this(
        pubsubFactory,
        topic,
        timestampAttribute,
        idAttribute,
        numShards,
        publishBatchSize,
        publishBatchBytes,
        DEFAULT_MAX_LATENCY,
        RecordIdMethod.RANDOM,
        null);
  }

  public PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      ValueProvider<TopicPath> topic,
      String timestampAttribute,
      String idAttribute,
      int numShards,
      int publishBatchSize,
      int publishBatchBytes,
      String pubsubRootUrl) {
    this(
        pubsubFactory,
        topic,
        timestampAttribute,
        idAttribute,
        numShards,
        publishBatchSize,
        publishBatchBytes,
        DEFAULT_MAX_LATENCY,
        RecordIdMethod.RANDOM,
        pubsubRootUrl);
  }
  /** Get the topic being written to. */
  public @Nullable TopicPath getTopic() {
    return topic != null ? topic.get() : null;
  }

  /** Get the {@link ValueProvider} for the topic being written to. */
  public @Nullable ValueProvider<TopicPath> getTopicProvider() {
    return topic;
  }

  /** Get the timestamp attribute. */
  public @Nullable String getTimestampAttribute() {
    return timestampAttribute;
  }

  /** Get the id attribute. */
  public @Nullable String getIdAttribute() {
    return idAttribute;
  }

  @Override
  public PDone expand(PCollection<PubsubMessage> input) {
    if (topic != null) {
      return input
          .apply(
              "Output Serialized PubsubMessage Proto",
              MapElements.into(new TypeDescriptor<byte[]>() {})
                  .via(new PubsubMessages.ParsePayloadAsPubsubMessageProto()))
          .setCoder(ByteArrayCoder.of())
          .apply(new PubsubSink(this));
    } else {
      // dynamic destinations.
      return input
          .apply(
              "WithDynamicTopics",
              WithKeys.of(PubsubMessage::getTopic).withKeyType(TypeDescriptors.strings()))
          .apply(
              MapValues.into(new TypeDescriptor<byte[]>() {})
                  .via(new PubsubMessages.ParsePayloadAsPubsubMessageProto()))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), ByteArrayCoder.of()))
          .apply(new PubsubDynamicSink(this));
    }
  }

  static class PubsubDynamicSink extends PTransform<PCollection<KV<String, byte[]>>, PDone> {
    public final PubsubUnboundedSink outer;

    PubsubDynamicSink(PubsubUnboundedSink outer) {
      this.outer = outer;
    }

    @Override
    public PDone expand(PCollection<KV<String, byte[]>> input) {
      input
          .apply(
              "PubsubUnboundedSink.Window",
              Window.<KV<String, byte[]>>into(new GlobalWindows())
                  .triggering(
                      Repeatedly.forever(
                          AfterFirst.of(
                              AfterPane.elementCountAtLeast(outer.publishBatchSize),
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(outer.maxLatency))))
                  .discardingFiredPanes())
          .apply(
              "PubsubUnboundedSink.ShardDynamicDestinations",
              ParDo.of(
                  new ShardFn<KV<String, byte[]>, KV<Integer, String>>(
                      outer.numShards,
                      outer.recordIdMethod,
                      kv -> {
                        try {
                          return com.google.pubsub.v1.PubsubMessage.parseFrom(kv.getValue());
                        } catch (InvalidProtocolBufferException e) {
                          throw new RuntimeException(e);
                        }
                      },
                      KV::getKey,
                      KV::of)))
          .setCoder(KvCoder.of(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()), CODER))
          .apply(GroupByKey.create())
          .apply(Values.create())
          .apply(
              "PubsubUnboundedSink.Writer",
              ParDo.of(
                  new WriterFn(
                      outer.pubsubFactory,
                      outer.topic,
                      outer.timestampAttribute,
                      outer.idAttribute,
                      outer.publishBatchSize,
                      outer.publishBatchBytes,
                      outer.pubsubRootUrl)));
      return PDone.in(input.getPipeline());
    }
  }

  static class PubsubSink extends PTransform<PCollection<byte[]>, PDone> {
    public final PubsubUnboundedSink outer;

    PubsubSink(PubsubUnboundedSink outer) {
      this.outer = outer;
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      input
          .apply(
              "PubsubUnboundedSink.Window",
              Window.<byte[]>into(new GlobalWindows())
                  .triggering(
                      Repeatedly.forever(
                          AfterFirst.of(
                              AfterPane.elementCountAtLeast(outer.publishBatchSize),
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(outer.maxLatency))))
                  .discardingFiredPanes())
          .apply(
              "PubsubUnboundedSink.Shard",
              ParDo.of(
                  new ShardFn<>(
                      outer.numShards,
                      outer.recordIdMethod,
                      m -> {
                        try {
                          return com.google.pubsub.v1.PubsubMessage.parseFrom(m);
                        } catch (InvalidProtocolBufferException e) {
                          throw new RuntimeException(e);
                        }
                      },
                      SerializableFunctions.constant(null),
                      (s, t) -> s)))
          .setCoder(KvCoder.of(VarIntCoder.of(), CODER))
          .apply(GroupByKey.create())
          .apply(Values.create())
          .apply(
              "PubsubUnboundedSink.Writer",
              ParDo.of(
                  new WriterFn(
                      outer.pubsubFactory,
                      outer.topic,
                      outer.timestampAttribute,
                      outer.idAttribute,
                      outer.publishBatchSize,
                      outer.publishBatchBytes,
                      outer.pubsubRootUrl)));
      return PDone.in(input.getPipeline());
    }
  }
}
