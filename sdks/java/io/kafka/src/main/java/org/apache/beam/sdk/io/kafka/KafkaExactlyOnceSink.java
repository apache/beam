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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO.WriteRecords;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exactly-once sink transform for Kafka. See {@link KafkaIO} for user visible documentation and
 * example usage.
 */
@SuppressWarnings({
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
class KafkaExactlyOnceSink<K, V>
    extends PTransform<PCollection<ProducerRecord<K, V>>, PCollection<Void>> {

  // Dataflow ensures at-least once processing for side effects like sinks. In order to provide
  // exactly-once semantics, a sink needs to be idempotent or it should avoid writing records
  // that have already been written. This snk does the latter. All the records are ordered
  // across a fixed number of shards and records in each shard are written in order. It drops
  // any records that are already written and buffers those arriving out of order.
  //
  // Exactly once sink involves two shuffles of the records:
  //    A : Assign a shard ---> B : Assign sequential ID ---> C : Write to Kafka in order
  //
  // Processing guarantees also require deterministic processing within user transforms.
  // Here, that requires order of the records committed to Kafka by C should not be affected by
  // restarts in C and its upstream stages.
  //
  // A : Assigns a random shard for message. Note that there are no ordering guarantees for
  //     writing user records to Kafka. User can still control partitioning among topic
  //     partitions as with regular sink (of course, there are no ordering guarantees in
  //     regular Kafka sink either).
  // B : Assigns an id sequentially for each messages within a shard.
  // C : Writes each shard to Kafka in sequential id order. In Dataflow, when C sees a record
  //     and id, it implies that record and the associated id are checkpointed to persistent
  //     storage and this record will always have same id, even in retries.
  //     Exactly-once semantics are achieved by writing records in the strict order of
  //     these check-pointed sequence ids.
  //
  // Parallelism for B and C is fixed to 'numShards', which defaults to number of partitions
  // for the topic. A few reasons for that:
  //  - B & C implement their functionality using per-key state. Shard id makes it independent
  //    of cardinality of user key.
  //  - We create one producer per shard, and its 'transactional id' is based on shard id. This
  //    requires that number of shards to be finite. This also helps with batching. and avoids
  //    initializing producers and transactions.
  //  - Most importantly, each of sharded writers stores 'next message id' in partition
  //    metadata, which is committed atomically with Kafka transactions. This is critical
  //    to handle retries of C correctly. Initial testing showed number of shards could be
  //    larger than number of partitions for the topic.
  //
  // Number of shards can change across multiple runs of a pipeline (job upgrade in Dataflow).
  //

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExactlyOnceSink.class);
  private static final String METRIC_NAMESPACE = "KafkaExactlyOnceSink";

  private final WriteRecords<K, V> spec;

  static void ensureEOSSupport() {
    checkArgument(
        ProducerSpEL.supportsTransactions(),
        "%s %s",
        "This version of Kafka client does not support transactions required to support",
        "exactly-once semantics. Please use Kafka client version 0.11 or newer.");
  }

  KafkaExactlyOnceSink(WriteRecords<K, V> spec) {
    this.spec = spec;
  }

  @Override
  public PCollection<Void> expand(PCollection<ProducerRecord<K, V>> input) {
    String topic = Preconditions.checkStateNotNull(spec.getTopic());

    int numShards = spec.getNumShards();
    if (numShards <= 0) {
      try (Consumer<?, ?> consumer = openConsumer(spec)) {
        numShards = consumer.partitionsFor(topic).size();
        LOG.info(
            "Using {} shards for exactly-once writer, matching number of partitions "
                + "for topic '{}'",
            numShards,
            spec.getTopic());
      }
    }
    checkState(numShards > 0, "Could not set number of shards");

    return input
        .apply(
            Window.<ProducerRecord<K, V>>into(new GlobalWindows()) // Everything into global window.
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes())
        .apply(
            String.format("Shuffle across %d shards", numShards),
            ParDo.of(new Reshard<>(numShards)))
        .apply("Persist sharding", GroupByKey.create())
        .apply("Assign sequential ids", ParDo.of(new Sequencer<>()))
        .apply("Persist ids", GroupByKey.create())
        .apply(
            String.format("Write to Kafka topic '%s'", spec.getTopic()),
            ParDo.of(new ExactlyOnceWriter<>(spec, input.getCoder())));
  }

  /** Shuffle messages assigning each randomly to a shard. */
  private static class Reshard<K, V>
      extends DoFn<ProducerRecord<K, V>, KV<Integer, TimestampedValue<ProducerRecord<K, V>>>> {

    private final int numShards;
    private transient int shardId;

    Reshard(int numShards) {
      this.numShards = numShards;
    }

    @Setup
    public void setup() {
      shardId = ThreadLocalRandom.current().nextInt(numShards);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      shardId = (shardId + 1) % numShards; // round-robin among shards.
      ctx.output(KV.of(shardId, TimestampedValue.of(ctx.element(), ctx.timestamp())));
    }
  }

  private static class Sequencer<K, V>
      extends DoFn<
          KV<Integer, Iterable<TimestampedValue<ProducerRecord<K, V>>>>,
          KV<Integer, KV<Long, TimestampedValue<ProducerRecord<K, V>>>>> {

    private static final String NEXT_ID = "nextId";

    @StateId(NEXT_ID)
    private final StateSpec<ValueState<Long>> nextIdSpec = StateSpecs.value();

    @ProcessElement
    public void processElement(@StateId(NEXT_ID) ValueState<Long> nextIdState, ProcessContext ctx) {
      long nextId = MoreObjects.firstNonNull(nextIdState.read(), 0L);
      int shard = ctx.element().getKey();
      for (TimestampedValue<ProducerRecord<K, V>> value : ctx.element().getValue()) {
        ctx.output(KV.of(shard, KV.of(nextId, value)));
        nextId++;
      }
      nextIdState.write(nextId);
    }
  }

  private static class ExactlyOnceWriter<K, V>
      extends DoFn<KV<Integer, Iterable<KV<Long, TimestampedValue<ProducerRecord<K, V>>>>>, Void> {

    private static final String NEXT_ID = "nextId";
    private static final String MIN_BUFFERED_ID = "minBufferedId";
    private static final String OUT_OF_ORDER_BUFFER = "outOfOrderBuffer";
    private static final String WRITER_ID = "writerId";

    // Not sure of a good limit. This applies only for large bundles.
    private static final int MAX_RECORDS_PER_TXN = 1000;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @StateId(NEXT_ID)
    private final StateSpec<ValueState<Long>> sequenceIdSpec = StateSpecs.value();

    @StateId(MIN_BUFFERED_ID)
    private final StateSpec<ValueState<Long>> minBufferedIdSpec = StateSpecs.value();

    @StateId(OUT_OF_ORDER_BUFFER)
    private final StateSpec<BagState<KV<Long, TimestampedValue<ProducerRecord<K, V>>>>>
        outOfOrderBufferSpec;
    // A random id assigned to each shard. Helps with detecting when multiple jobs are mistakenly
    // started with same groupId used for storing state on Kafka side, including the case where
    // a job is restarted with same groupId, but the metadata from previous run was not cleared.
    // Better to be safe and error out with a clear message.

    @StateId(WRITER_ID)
    private final StateSpec<ValueState<String>> writerIdSpec = StateSpecs.value();

    private final WriteRecords<K, V> spec;

    // Metrics
    private final Counter elementsWritten = SinkMetrics.elementsWritten();
    // Elements buffered due to out of order arrivals.
    private final Counter elementsBuffered = Metrics.counter(METRIC_NAMESPACE, "elementsBuffered");
    private final Counter numTransactions = Metrics.counter(METRIC_NAMESPACE, "numTransactions");

    ExactlyOnceWriter(WriteRecords<K, V> spec, Coder<ProducerRecord<K, V>> elemCoder) {
      this.spec = spec;
      this.outOfOrderBufferSpec =
          StateSpecs.bag(KvCoder.of(BigEndianLongCoder.of(), TimestampedValueCoder.of(elemCoder)));
    }

    @Setup
    public void setup() {
      // This is on the worker. Ensure the runtime version is till compatible.
      KafkaExactlyOnceSink.ensureEOSSupport();
    }

    // Futures ignored as exceptions will be flushed out in the commitTxn
    @SuppressWarnings("FutureReturnValueIgnored")
    @RequiresStableInput
    @ProcessElement
    public void processElement(
        @StateId(NEXT_ID) ValueState<Long> nextIdState,
        @StateId(MIN_BUFFERED_ID) ValueState<Long> minBufferedIdState,
        @StateId(OUT_OF_ORDER_BUFFER)
            BagState<KV<Long, TimestampedValue<ProducerRecord<K, V>>>> oooBufferState,
        @StateId(WRITER_ID) ValueState<String> writerIdState,
        ProcessContext ctx)
        throws IOException {

      int shard = ctx.element().getKey();

      minBufferedIdState.readLater();
      long nextId = MoreObjects.firstNonNull(nextIdState.read(), 0L);
      long minBufferedId = MoreObjects.firstNonNull(minBufferedIdState.read(), Long.MAX_VALUE);

      String sinkGroupId = Preconditions.checkStateNotNull(spec.getSinkGroupId());
      ShardWriterCache<K, V> cache =
          (ShardWriterCache<K, V>) CACHE_BY_GROUP_ID.getUnchecked(sinkGroupId);
      ShardWriter<K, V> writer = cache.removeIfPresent(shard);
      if (writer == null) {
        writer = initShardWriter(shard, writerIdState, nextId);
      }

      long committedId = writer.committedId;

      if (committedId >= nextId) {
        // This is a retry of an already committed batch.
        LOG.info(
            "{}: committed id {} is ahead of expected {}. {} records will be dropped "
                + "(these are already written).",
            shard,
            committedId,
            nextId - 1,
            committedId - nextId + 1);
        nextId = committedId + 1;
      }

      try {
        writer.beginTxn();
        int txnSize = 0;

        // Iterate in recordId order. The input iterator could be mostly sorted.
        // There might be out of order messages buffered in earlier iterations. These
        // will get merged if and when minBufferedId matches nextId.

        Iterator<KV<Long, TimestampedValue<ProducerRecord<K, V>>>> iter =
            ctx.element().getValue().iterator();

        while (iter.hasNext()) {
          KV<Long, TimestampedValue<ProducerRecord<K, V>>> kv = iter.next();
          long recordId = kv.getKey();

          if (recordId < nextId) {
            LOG.info(
                "{}: dropping older record {}. Already committed till {}",
                shard,
                recordId,
                committedId);
            continue;
          }

          if (recordId > nextId) {
            // Out of order delivery. Should be pretty rare (what about in a batch pipeline?)
            LOG.info(
                "{}: Saving out of order record {}, next record id to be written is {}",
                shard,
                recordId,
                nextId);

            // checkState(recordId - nextId < 10000, "records are way out of order");

            oooBufferState.add(kv);
            minBufferedId = Math.min(minBufferedId, recordId);
            minBufferedIdState.write(minBufferedId);
            elementsBuffered.inc();
            continue;
          }

          // recordId and nextId match. Finally write the record.

          writer.sendRecord(kv.getValue(), elementsWritten);
          nextId++;

          if (++txnSize >= MAX_RECORDS_PER_TXN) {
            writer.commitTxn(recordId, numTransactions);
            txnSize = 0;
            writer.beginTxn();
          }

          if (minBufferedId == nextId) {
            // One or more of the buffered records can be committed now.
            // Read all of them in to memory and sort them. Reading into memory
            // might be problematic in extreme cases. Might need to improve it in future.

            List<KV<Long, TimestampedValue<ProducerRecord<K, V>>>> buffered =
                Lists.newArrayList(oooBufferState.read());
            buffered.sort(new KV.OrderByKey<>());

            LOG.info(
                "{} : merging {} buffered records (min buffered id is {}).",
                shard,
                buffered.size(),
                minBufferedId);

            oooBufferState.clear();
            minBufferedIdState.clear();
            minBufferedId = Long.MAX_VALUE;

            iter =
                Iterators.mergeSorted(
                    ImmutableList.of(iter, buffered.iterator()), new KV.OrderByKey<>());
          }
        }

        writer.commitTxn(nextId - 1, numTransactions);
        nextIdState.write(nextId);

      } catch (ProducerSpEL.UnrecoverableProducerException e) {
        // Producer JavaDoc says these are not recoverable errors and producer should be closed.

        // Close the producer and a new producer will be initialized in retry.
        // It is possible that a rough worker keeps retrying and ends up fencing off
        // active producers. How likely this might be or how well such a scenario is handled
        // depends on the runner. For now we will leave it to upper layers, will need to revisit.

        LOG.warn(
            "{} : closing producer {} after unrecoverable error. The work might have migrated."
                + " Committed id {}, current id {}.",
            writer.shard,
            writer.producerName,
            writer.committedId,
            nextId - 1,
            e);

        writer.producer.close();
        writer = null; // No need to cache it.
        throw e;
      } finally {
        if (writer != null) {
          cache.insert(shard, writer);
        }
      }
    }

    private static class ShardMetadata {

      @JsonProperty("seq")
      public final long sequenceId;

      @JsonProperty("id")
      public final @Nullable String writerId;

      private ShardMetadata() { // for json deserializer
        sequenceId = -1;
        writerId = null;
      }

      ShardMetadata(long sequenceId, String writerId) {
        this.sequenceId = sequenceId;
        this.writerId = writerId;
      }
    }

    /** A wrapper around Kafka producer. One for each of the shards. */
    private static class ShardWriter<K, V> {

      private final int shard;
      private final String writerId;
      private final Producer<K, V> producer;
      private final String producerName;
      private final WriteRecords<K, V> spec;
      private long committedId;
      private transient boolean reportedLineage;

      ShardWriter(
          int shard,
          String writerId,
          Producer<K, V> producer,
          String producerName,
          WriteRecords<K, V> spec,
          long committedId) {
        this.shard = shard;
        this.writerId = writerId;
        this.producer = producer;
        this.producerName = producerName;
        this.spec = spec;
        this.committedId = committedId;
      }

      void beginTxn() {
        ProducerSpEL.beginTransaction(producer);
      }

      Future<RecordMetadata> sendRecord(
          TimestampedValue<ProducerRecord<K, V>> record, Counter sendCounter) {
        String topic = Preconditions.checkStateNotNull(spec.getTopic());
        try {
          Long timestampMillis =
              spec.getPublishTimestampFunction() != null
                  ? spec.getPublishTimestampFunction()
                      .getTimestamp(record.getValue(), record.getTimestamp())
                      .getMillis()
                  : null;

          @SuppressWarnings("nullness") // Kafka library not annotated
          Future<RecordMetadata> result =
              producer.send(
                  new ProducerRecord<>(
                      topic,
                      null,
                      timestampMillis,
                      record.getValue().key(),
                      record.getValue().value()));
          sendCounter.inc();
          return result;
        } catch (KafkaException e) {
          ProducerSpEL.abortTransaction(producer);
          throw e;
        }
      }

      void commitTxn(long lastRecordId, Counter numTransactions) throws IOException {
        String topic = Preconditions.checkStateNotNull(spec.getTopic());
        try {
          // Store id in consumer group metadata for the partition.
          // NOTE: Kafka keeps this metadata for 24 hours since the last update. This limits
          // how long the pipeline could be down before resuming it. It does not look like
          // this TTL can be adjusted (asked about it on Kafka users list).
          ProducerSpEL.sendOffsetsToTransaction(
              producer,
              ImmutableMap.of(
                  new TopicPartition(topic, shard),
                  new OffsetAndMetadata(
                      0L,
                      JSON_MAPPER.writeValueAsString(new ShardMetadata(lastRecordId, writerId)))),
              spec.getSinkGroupId());
          ProducerSpEL.commitTransaction(producer);

          numTransactions.inc();
          if (!reportedLineage) {
            Lineage.getSinks()
                .add(
                    "kafka",
                    ImmutableList.of(
                        // withBootstrapServers() was required in WriteRecord.expand, expect to be
                        // non-null
                        (String)
                            Preconditions.checkStateNotNull(
                                spec.getProducerConfig()
                                    .get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)),
                        topic));
            reportedLineage = true;
          }
          LOG.debug("{} : committed {} records", shard, lastRecordId - committedId);

          committedId = lastRecordId;
        } catch (KafkaException e) {
          ProducerSpEL.abortTransaction(producer);
          throw e;
        }
      }
    }

    private ShardWriter<K, V> initShardWriter(
        int shard, ValueState<String> writerIdState, long nextId) throws IOException {

      String producerName = String.format("producer_%d_for_%s", shard, spec.getSinkGroupId());
      Producer<K, V> producer = initializeExactlyOnceProducer(spec, producerName);
      String topic = Preconditions.checkStateNotNull(spec.getTopic());

      // Fetch latest committed metadata for the partition (if any). Checks committed sequence ids.
      try {

        String writerId = writerIdState.read();

        OffsetAndMetadata committed;

        try (Consumer<?, ?> consumer = openConsumer(spec)) {
          committed = consumer.committed(new TopicPartition(topic, shard));
        }

        long committedSeqId = -1;

        if (committed == null || committed.metadata() == null || committed.metadata().isEmpty()) {
          checkState(
              nextId == 0 && writerId == null,
              "State exists for shard %s (nextId %s, writerId '%s'), but there is no state "
                  + "stored with Kafka topic '%s' group id '%s'",
              shard,
              nextId,
              writerId,
              spec.getTopic(),
              spec.getSinkGroupId());

          writerId =
              String.format(
                  "%X - %s",
                  new Random().nextInt(Integer.MAX_VALUE),
                  DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
                      .withZone(DateTimeZone.UTC)
                      .print(DateTimeUtils.currentTimeMillis()));
          writerIdState.write(writerId);
          LOG.info("Assigned writer id '{}' to shard {}", writerId, shard);

        } else {
          ShardMetadata metadata = JSON_MAPPER.readValue(committed.metadata(), ShardMetadata.class);

          checkNotNull(metadata.writerId);

          if (writerId == null) {
            // a) This might be a restart of the job from scratch, in which case metatdata
            // should be ignored and overwritten with new one.
            // b) This job might be started with an incorrect group id which is an error.
            // c) There is an extremely small chance that this is a retry of the first bundle
            // where metatdate was committed to Kafka but the bundle results were not committed
            // in Beam, in which case it should be treated as correct metadata.
            // How can we tell these three cases apart? Be safe and throw an exception.
            //
            // We could let users explicitly an option to override the existing metadata.
            //
            throw new IllegalStateException(
                String.format(
                    "Kafka metadata exists for shard %s, but there is no stored state for it. "
                        + "This mostly indicates groupId '%s' is used else where or in earlier runs. "
                        + "Try another group id. Metadata for this shard on Kafka : '%s'",
                    shard, spec.getSinkGroupId(), committed.metadata()));
          }

          checkState(
              writerId.equals(metadata.writerId),
              "Writer ids don't match. This is mostly a unintended misuse of groupId('%s')."
                  + "Beam '%s', Kafka '%s'",
              spec.getSinkGroupId(),
              writerId,
              metadata.writerId);

          committedSeqId = metadata.sequenceId;

          checkState(
              committedSeqId >= (nextId - 1),
              "Committed sequence id can not be lower than %s, partition metadata : %s",
              nextId - 1,
              committed.metadata());
        }

        LOG.info(
            "{} : initialized producer {} with committed sequence id {}",
            shard,
            producerName,
            committedSeqId);

        return new ShardWriter<>(shard, writerId, producer, producerName, spec, committedSeqId);

      } catch (IOException e) {
        producer.close();
        throw e;
      }
    }

    /**
     * A wrapper around guava cache to provide insert()/remove() semantics. A ShardWriter will be
     * closed if it is stays in cache for more than 1 minute, i.e. not used inside
     * KafkaExactlyOnceSink DoFn for a minute.
     */
    private static class ShardWriterCache<K, V> {

      static final ScheduledExecutorService SCHEDULED_CLEAN_UP_THREAD =
          Executors.newSingleThreadScheduledExecutor();

      static final int CLEAN_UP_CHECK_INTERVAL_MS = 10 * 1000;
      static final int IDLE_TIMEOUT_MS = 60 * 1000;

      private final Cache<Integer, ShardWriter<K, V>> cache;

      // Exceptions arising from the cache cleanup are ignored
      @SuppressWarnings("FutureReturnValueIgnored")
      ShardWriterCache() {
        this.cache =
            CacheBuilder.newBuilder()
                .expireAfterWrite(IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .<Integer, ShardWriter<K, V>>removalListener(
                    notification -> {
                      if (notification.getCause() != RemovalCause.EXPLICIT) {
                        ShardWriter<K, V> writer = checkNotNull(notification.getValue());
                        LOG.info(
                            "{} : Closing idle shard writer {} after 1 minute of idle time.",
                            writer.shard,
                            writer.producerName);
                        writer.producer.close();
                      }
                    })
                .build();

        // run cache.cleanUp() every 10 seconds.
        SCHEDULED_CLEAN_UP_THREAD.scheduleAtFixedRate(
            cache::cleanUp,
            CLEAN_UP_CHECK_INTERVAL_MS,
            CLEAN_UP_CHECK_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
      }

      @Nullable
      ShardWriter<K, V> removeIfPresent(int shard) {
        return cache.asMap().remove(shard);
      }

      void insert(int shard, ShardWriter<K, V> writer) {
        ShardWriter<K, V> existing = cache.asMap().putIfAbsent(shard, writer);
        checkState(
            existing == null, "Unexpected multiple instances of writers for shard %s", shard);
      }
    }

    // One cache for each sink (usually there is only one sink per pipeline)
    private static final LoadingCache<String, ShardWriterCache<?, ?>> CACHE_BY_GROUP_ID =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, ShardWriterCache<?, ?>>() {
                  @Override
                  public ShardWriterCache<?, ?> load(String key) throws Exception {
                    return new ShardWriterCache<>();
                  }
                });
  }

  /**
   * Opens a generic consumer that is mainly meant for metadata operations like fetching number of
   * partitions for a topic rather than for fetching messages.
   */
  private static Consumer<?, ?> openConsumer(WriteRecords<?, ?> spec) {
    SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>> consumerFactoryFn =
        Preconditions.checkArgumentNotNull(spec.getConsumerFactoryFn());

    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        Preconditions.checkArgumentNotNull(
            spec.getProducerConfig().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)));
    if (spec.getSinkGroupId() != null) {
      consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, spec.getSinkGroupId());
    }
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    return consumerFactoryFn.apply(consumerConfig);
  }

  private static <K, V> Producer<K, V> initializeExactlyOnceProducer(
      WriteRecords<K, V> spec, String producerName) {

    Map<String, Object> producerConfig = new HashMap<>(spec.getProducerConfig());
    if (spec.getKeySerializer() != null) {
      producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, spec.getKeySerializer());
    }
    if (spec.getValueSerializer() != null) {
      producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, spec.getValueSerializer());
    }
    producerConfig.put(ProducerSpEL.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerConfig.put(ProducerSpEL.TRANSACTIONAL_ID_CONFIG, producerName);

    Producer<K, V> producer =
        spec.getProducerFactoryFn() != null
            ? spec.getProducerFactoryFn().apply(producerConfig)
            : new KafkaProducer<>(producerConfig);

    ProducerSpEL.initTransactions(producer);
    return producer;
  }
}
