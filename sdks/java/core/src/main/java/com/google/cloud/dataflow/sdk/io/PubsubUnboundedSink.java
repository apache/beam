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

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterFirst;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
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
 * A PTransform which streams messages to pub/sub.
 * <ul>
 * <li>The underlying implementation is just a {@link DoFn} which publishes as a side effect.
 * <li>We use gRPC for its speed and low memory overhead.
 * <li>We try to send messages in batches while also limiting send latency.
 * <li>No stats are logged. Rather some counters are used to keep track of elements and batches.
 * <li>Though some background threads are used by the underlying netty system all actual pub/sub
 * calls are blocking. We rely on the underlying runner to allow multiple {@link DoFn} instances
 * to execute concurrently and thus hide latency.
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
   * Longest delay between receiving a message and pushing it to pub/sub.
   */
  private static final Duration MAX_LATENCY = Duration.standardSeconds(2);

  /**
   * Period of samples for stats.
   */
  private static final Duration SAMPLE_PERIOD = Duration.standardMinutes(1);

  /**
   * Period of updates for stats.
   */
  private static final Duration SAMPLE_UPDATE = Duration.standardSeconds(5);

  /**
   * How frequently to log stats.
   */
  private static final Duration LOG_PERIOD = Duration.standardSeconds(30);

  /**
   * Additional sharding so that we can hide publish message latency.
   */
  private static final int SCALE_OUT = 4;

  public static final Coder<PubsubClient.OutgoingMessage> CODER = new
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
    /**
     * Number of cores available for publishing.
     */
    private final int numCores;

    private final Aggregator<Long, Long> elementCounter =
        createAggregator("elements", new Sum.SumLongFn());

    public ShardFn(int numCores) {
      this.numCores = numCores;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      elementCounter.addValue(1L);
      byte[] elementBytes = CoderUtils.encodeToByteArray(elementCoder, c.element());
      long timestampMsSinceEpoch = c.timestamp().getMillis();
      c.output(KV.of(ThreadLocalRandom.current().nextInt(numCores * SCALE_OUT),
                     new PubsubClient.OutgoingMessage(elementBytes, timestampMsSinceEpoch)));
    }
  }

  // ================================================================================
  // WriterFn
  // ================================================================================

  /**
   * Publish messages to pub/sub in batches.
   */
  private class WriterFn
      extends DoFn<KV<Integer, Iterable<PubsubClient.OutgoingMessage>>, Void> {

    /**
     * Client on which to talk to pub/sub. Null until created by {@link #startBundle}.
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
     * Send {@code messages} as a batch to pub/sub.
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
      pubsubClient = PubsubGrpcClient.newClient(timestampLabel, idLabel,
                                                c.getPipelineOptions().as(GcpOptions.class));
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
          // (We've already broken by batch size using the trigger belom, though that may
          // run slightly over the actual PUBLISH_BATCH_SIZE)
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
   * Number of cores available for publishing.
   */
  private final int numCores;

  /**
   * Pub/sub topic to publish to.
   */
  private final String topic;

  /**
   * Coder for elements. Elements are effectively double-encoded: first to a byte array
   * using this checkpointCoder, then to a base-64 string to conform to pub/sub's payload
   * conventions.
   */
  private final Coder<T> elementCoder;

  /**
   * Pub/sub metadata field holding timestamp of each element, or {@literal null} if should use
   * pub/sub message publish timestamp instead.
   */
  @Nullable
  private final String timestampLabel;

  /**
   * Pub/sub metadata field holding id for each element, or {@literal null} if need to generate
   * a unique id ourselves.
   * CAUTION: Currently ignored.
   */
  @Nullable
  private final String idLabel;

  public PubsubUnboundedSink(
      int numCores,
      String topic,
      Coder<T> elementCoder,
      String timestampLabel,
      String idLabel) {
    this.numCores = numCores;
    this.topic = topic;
    this.elementCoder = elementCoder;
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
  }

  @Override
  public PDone apply(PCollection<T> input) {
    String label = "PubsubSink(" + topic.replaceFirst(".*/", "") + ")";
    input.apply(Window.<T>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(
                            AfterFirst.of(AfterPane.elementCountAtLeast(PUBLISH_BATCH_SIZE),
                                          AfterProcessingTime.pastFirstElementInPane()
                                                             .plusDelayOf(MAX_LATENCY))))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
         .apply(ParDo.named(label + ".Shard").of(new ShardFn(numCores)))
         .setCoder(KvCoder.of(VarIntCoder.of(), CODER))
         .apply(GroupByKey.<Integer, PubsubClient.OutgoingMessage>create())
         .apply(ParDo.named(label + ".Writer").of(new WriterFn()));
    return PDone.in(input.getPipeline());
  }
}
