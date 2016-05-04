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

package org.apache.beam.sdk.io;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PubsubOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.PubsubClient;
import org.apache.beam.sdk.util.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.common.base.Preconditions;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

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
 * <li>A failed work item will cause messages to be resent. Thus we rely on the Pubsub consumer
 * to dedup messages.
 * </ul>
 */
public class PubsubUnboundedSink<T> extends PTransform<PCollection<T>, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubUnboundedSink.class);

  /**
   * Maximum number of messages per publish.
   */
  private static final int PUBLISH_BATCH_SIZE = 1000;

  /**
   * Maximum size of a publish batch, in bytes.
   */
  private static final long PUBLISH_BATCH_BYTES = 400000;

  /**
   * Longest delay between receiving a message and pushing it to Pubsub.
   */
  private static final Duration MAX_LATENCY = Duration.standardSeconds(2);

  /**
   * Coder for conveying outgoing messages between internal stages.
   */
  private static final Coder<PubsubClient.OutgoingMessage> CODER = new
      AtomicCoder<PubsubClient.OutgoingMessage>() {
        @Override
        public void encode(
            PubsubClient.OutgoingMessage value, OutputStream outStream, Context context)
            throws CoderException, IOException {
          ByteArrayCoder.of().encode(value.elementBytes, outStream, Context.NESTED);
          BigEndianLongCoder.of().encode(value.timestampMsSinceEpoch, outStream, Context.NESTED);
        }

        @Override
        public PubsubClient.OutgoingMessage decode(
            InputStream inStream, Context context) throws CoderException, IOException {
          byte[] elementBytes = ByteArrayCoder.of().decode(inStream, Context.NESTED);
          long timestampMsSinceEpoch = BigEndianLongCoder.of().decode(inStream, Context.NESTED);
          return new PubsubClient.OutgoingMessage(elementBytes, timestampMsSinceEpoch);
        }
      };

  // ================================================================================
  // ShardFv
  // ================================================================================

  /**
   * Convert elements to messages and shard them.
   */
  private class ShardFn extends DoFn<T, KV<Integer, PubsubClient.OutgoingMessage>> {
    private final Aggregator<Long, Long> elementCounter =
        createAggregator("elements", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) throws Exception {
      elementCounter.addValue(1L);
      byte[] elementBytes = CoderUtils.encodeToByteArray(elementCoder, c.element());
      long timestampMsSinceEpoch = c.timestamp().getMillis();
      c.output(KV.of(ThreadLocalRandom.current().nextInt(numShards),
                     new PubsubClient.OutgoingMessage(elementBytes, timestampMsSinceEpoch)));
    }
  }

  // ================================================================================
  // WriterFn
  // ================================================================================

  /**
   * Publish messages to Pubsub in batches.
   */
  private class WriterFn
      extends DoFn<KV<Integer, Iterable<PubsubClient.OutgoingMessage>>, Void> {

    /**
     * Client on which to talk to Pubsub. Null until created by {@link #startBundle}.
     */
    @Nullable
    private transient PubsubClient pubsubClient;

    private final Aggregator<Long, Long> batchCounter =
        createAggregator("batches", new Sum.SumLongFn());
    private final Aggregator<Long, Long> elementCounter =
        createAggregator("elements", new Sum.SumLongFn());
    private final Aggregator<Long, Long> byteCounter =
        createAggregator("bytes", new Sum.SumLongFn());

    /**
     * BLOCKING
     * Send {@code messages} as a batch to Pubsub.
     */
    private void publishBatch(List<PubsubClient.OutgoingMessage> messages, int bytes)
        throws IOException {
      long nowMsSinceEpoch = System.currentTimeMillis();
      int n = pubsubClient.publish(topic, messages);
      Preconditions.checkState(n == messages.size());
      batchCounter.addValue(1L);
      elementCounter.addValue((long) messages.size());
      byteCounter.addValue((long) bytes);
    }

    @Override
    public void startBundle(Context c) throws Exception {
      Preconditions.checkState(pubsubClient == null);
      pubsubClient = pubsubFactory.newClient(timestampLabel, idLabel,
                                             c.getPipelineOptions().as(PubsubOptions.class));
      super.startBundle(c);
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      List<PubsubClient.OutgoingMessage> pubsubMessages = new ArrayList<>(PUBLISH_BATCH_SIZE);
      int bytes = 0;
      for (PubsubClient.OutgoingMessage message : c.element().getValue()) {
        if (!pubsubMessages.isEmpty()
            && bytes + message.elementBytes.length > PUBLISH_BATCH_BYTES) {
          // Break large (in bytes) batches into smaller.
          // (We've already broken by batch size using the trigger below, though that may
          // run slightly over the actual PUBLISH_BATCH_SIZE. There is currently no way to
          // trigger on accumulated message size.)
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

    @Override
    public void finishBundle(Context c) throws Exception {
      pubsubClient.close();
      pubsubClient = null;
      super.finishBundle(c);
    }
  }


  // ================================================================================
  // PubsubUnboundedSink
  // ================================================================================

  /**
   * Which factor to use for creating Pubsub transport.
   */
  private final PubsubClientFactory pubsubFactory;

  /**
   * Pubsub topic to publish to.
   */
  private final PubsubClient.TopicPath topic;

  /**
   * Coder for elements. It is the responsibility of the underlying Pubsub transport to
   * re-encode element bytes if necessary, eg as Base64 strings.
   */
  private final Coder<T> elementCoder;

  /**
   * Pubsub metadata field holding timestamp of each element, or {@literal null} if should use
   * Pubsub message publish timestamp instead.
   */
  @Nullable
  private final String timestampLabel;

  /**
   * Pubsub metadata field holding id for each element, or {@literal null} if need to generate
   * a unique id ourselves.
   */
  @Nullable
  private final String idLabel;

  /**
   * Number of 'shards' to use so that latency in Pubsub publish can be hidden. Generally this
   * should be a small multiple of the number of available cores. Too smoll a number results
   * in too much time lost to blocking Pubsub calls. To large a number results in too many
   * single-element batches being sent to Pubsub with high per-batch overhead.
   */
  private final int numShards;

  public PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      PubsubClient.TopicPath topic,
      Coder<T> elementCoder,
      String timestampLabel,
      String idLabel,
      int numShards) {
    this.pubsubFactory = pubsubFactory;
    this.topic = topic;
    this.elementCoder = elementCoder;
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    this.numShards = numShards;
  }

  public PubsubClient.TopicPath getTopic() {
    return topic;
  }

  @Nullable
  public String getTimestampLabel() {
    return timestampLabel;
  }

  @Nullable
  public String getIdLabel() {
    return idLabel;
  }

  public Coder<T> getElementCoder() {
    return elementCoder;
  }

  @Override
  public PDone apply(PCollection<T> input) {
    // TODO: Include topic.getPath() in transform metadata when it is available.
    String label = "PubsubSink";
    input.apply(
        Window.named(label + ".Window")
            .<T>into(new GlobalWindows())
            .triggering(
                Repeatedly.forever(
                    AfterFirst.of(AfterPane.elementCountAtLeast(PUBLISH_BATCH_SIZE),
                                  AfterProcessingTime.pastFirstElementInPane()
                                                     .plusDelayOf(MAX_LATENCY))))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.ZERO))
         .apply(ParDo.named(label + ".Shard").of(new ShardFn()))
         .setCoder(KvCoder.of(VarIntCoder.of(), CODER))
         .apply(GroupByKey.<Integer, PubsubClient.OutgoingMessage>create())
         .apply(ParDo.named(label + ".Writer").of(new WriterFn()));
    return PDone.in(input.getPipeline());
  }
}
