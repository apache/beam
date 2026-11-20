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
package org.apache.beam.sdk.io.amqp;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;
import org.apache.qpid.proton.messenger.Tracker;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * AmqpIO supports AMQP 1.0 protocol using the Apache QPid Proton-J library.
 *
 * <p>It's also possible to use AMQP 1.0 protocol via Apache Qpid JMS connection factory and the
 * Apache Beam JmsIO.
 *
 * <h3>Binding AMQP and receive messages</h3>
 *
 * <p>The {@link AmqpIO} {@link Read} can bind a AMQP listener endpoint and receive messages. It can
 * also connect to a AMPQ broker (such as Apache Qpid or Apache ActiveMQ).
 *
 * <p>{@link AmqpIO} {@link Read} returns an unbounded {@link PCollection} of {@link Message}
 * containing the received messages.
 *
 * <p>To configure a AMQP source, you have to provide a list of addresses where it will receive
 * messages. An address has the following form: {@code
 * [amqp[s]://][user[:password]@]domain[/[name]]} where {@code domain} can be one of {@code
 * host | host:port | ip | ip:port | name}. NB: the {@code ~} character allows to bind a AMQP
 * listener instead of connecting to a remote broker. For instance {@code amqp://~0.0.0.0:1234} will
 * bind a AMQP listener on any network interface on the 1234 port number.
 *
 * <p>The following example illustrates how to configure a AMQP source:
 *
 * <pre>{@code
 * pipeline.apply(AmqpIO.read()
 *   .withAddresses(Collections.singletonList("amqp://host:1234")))
 *
 * }</pre>
 *
 * <h3>Sending messages to a AMQP endpoint</h3>
 *
 * <p>{@link AmqpIO} provides a sink to send {@link PCollection} elements as messages.
 *
 * <p>As for the {@link Read}, {@link AmqpIO} {@link Write} requires a list of addresses where to
 * send messages. The following example illustrates how to configure the {@link AmqpIO} {@link
 * Write}:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // provide PCollection<Message>
 *   .apply(AmqpIO.write());
 *
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class AmqpIO {

  public static Read read() {
    return new AutoValue_AmqpIO_Read.Builder().setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_AmqpIO_Write();
  }

  private AmqpIO() {}

  /** A {@link PTransform} to read/receive messages using AMQP 1.0 protocol. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Message>> {

    abstract @Nullable List<String> addresses();

    abstract long maxNumRecords();

    abstract @Nullable Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
    }

    /** Define the AMQP addresses where to receive messages. */
    public Read withAddresses(List<String> addresses) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(!addresses.isEmpty(), "addresses can not be empty");
      return builder().setAddresses(addresses).build();
    }

    /**
     * Define the max number of records received by the {@link Read}. When the max number of records
     * is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
     * PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages. When this
     * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("addresses", Joiner.on(" ").join(addresses())));
    }

    @Override
    public PCollection<Message> expand(PBegin input) {
      checkArgument(addresses() != null, "withAddresses() is required");

      org.apache.beam.sdk.io.Read.Unbounded<Message> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedAmqpSource(this));

      PTransform<PBegin, PCollection<Message>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }
  }

  @VisibleForTesting
  static class AmqpCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

    // Pinned to the value computed before readerId and sequence were added, so checkpoints written
    // by older versions can still be decoded.
    private static final long serialVersionUID = 6903044759462496471L;

    private final @Nullable String readerId;
    private final long sequence;
    private transient @Nullable Messenger messenger;
    private transient List<Tracker> trackers = new ArrayList<>();
    private transient @Nullable PendingCheckpoints pendingCheckpoints;
    private transient @Nullable AtomicInteger activeCheckpoints;
    private transient @Nullable Runnable onCheckpointFinalized;

    public AmqpCheckpointMark() {
      this(null, 0, null, new ArrayList<>(), null, null);
    }

    private AmqpCheckpointMark(
        @Nullable PendingCheckpoints pendingCheckpoints,
        long sequence,
        @Nullable Messenger messenger,
        List<Tracker> trackers,
        @Nullable AtomicInteger activeCheckpoints,
        @Nullable Runnable onCheckpointFinalized) {
      this.pendingCheckpoints = pendingCheckpoints;
      this.readerId = pendingCheckpoints == null ? null : pendingCheckpoints.readerId;
      this.sequence = sequence;
      this.messenger = messenger;
      this.trackers = trackers;
      this.activeCheckpoints = activeCheckpoints;
      this.onCheckpointFinalized = onCheckpointFinalized;
    }

    @Override
    public void finalizeCheckpoint() {
      PendingCheckpoints pending = pendingCheckpoints;
      if (pending == null && readerId != null) {
        pending = PendingCheckpoints.LIVE.getIfPresent(readerId);
      }
      if (pending != null) {
        for (AmqpCheckpointMark checkpoint : pending.removeUpTo(sequence)) {
          checkpoint.acknowledge();
        }
        return;
      }
      if (readerId != null) {
        return;
      }
      acknowledge();
    }

    private synchronized void acknowledge() {
      if (messenger != null) {
        for (Tracker tracker : trackers) {
          // flag as not cumulative
          messenger.accept(tracker, 0);
        }
      }
      trackers.clear();
      messenger = null;
      release(true);
    }

    private synchronized void abandon() {
      trackers.clear();
      messenger = null;
      release(false);
    }

    private void release(boolean notifyIfDrained) {
      if (activeCheckpoints != null) {
        int remaining = activeCheckpoints.decrementAndGet();
        activeCheckpoints = null;
        if (notifyIfDrained && remaining == 0 && onCheckpointFinalized != null) {
          onCheckpointFinalized.run();
        }
      }
    }

    // set an empty list to messages when deserialize
    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      trackers = new ArrayList<>();
    }

    private static final class PendingCheckpoints {
      private static final Cache<String, PendingCheckpoints> LIVE =
          CacheBuilder.newBuilder().weakValues().build();

      private final String readerId = UUID.randomUUID().toString();
      private final List<AmqpCheckpointMark> checkpoints = new ArrayList<>();
      private long nextSequence;

      static PendingCheckpoints create() {
        PendingCheckpoints pending = new PendingCheckpoints();
        LIVE.put(pending.readerId, pending);
        return pending;
      }

      synchronized long nextSequence() {
        return nextSequence++;
      }

      synchronized void add(AmqpCheckpointMark checkpoint) {
        checkpoints.add(checkpoint);
      }

      synchronized List<AmqpCheckpointMark> removeUpTo(long sequence) {
        List<AmqpCheckpointMark> removed = new ArrayList<>();
        checkpoints.removeIf(
            checkpoint -> {
              if (checkpoint.sequence <= sequence) {
                removed.add(checkpoint);
                return true;
              }
              return false;
            });
        return removed;
      }

      synchronized List<AmqpCheckpointMark> removeAll() {
        LIVE.invalidate(readerId);
        List<AmqpCheckpointMark> removed = new ArrayList<>(checkpoints);
        checkpoints.clear();
        return removed;
      }
    }

    static class Preparer {
      private final PendingCheckpoints pendingCheckpoints = PendingCheckpoints.create();
      private final List<Tracker> trackers = new ArrayList<>();
      private boolean discarded = false;

      synchronized void add(Tracker tracker) {
        if (discarded) {
          throw new IllegalStateException("Attempting to add tracker to discarded checkpoint");
        }
        trackers.add(tracker);
      }

      synchronized void discard() {
        trackers.clear();
        discarded = true;
      }

      synchronized AmqpCheckpointMark newCheckpoint(
          @Nullable Messenger messenger,
          @Nullable AtomicInteger activeCheckpoints,
          @Nullable Runnable onCheckpointFinalized) {
        if (discarded || trackers.isEmpty()) {
          trackers.clear();
          return new AmqpCheckpointMark(
              pendingCheckpoints,
              pendingCheckpoints.nextSequence(),
              null,
              new ArrayList<>(),
              null,
              null);
        }
        List<Tracker> snapshot = new ArrayList<>(trackers);
        trackers.clear();
        AmqpCheckpointMark mark =
            new AmqpCheckpointMark(
                pendingCheckpoints,
                pendingCheckpoints.nextSequence(),
                messenger,
                snapshot,
                activeCheckpoints,
                onCheckpointFinalized);
        pendingCheckpoints.add(mark);
        if (activeCheckpoints != null) {
          activeCheckpoints.incrementAndGet();
        }
        return mark;
      }

      void abandonPendingCheckpoints() {
        for (AmqpCheckpointMark checkpoint : pendingCheckpoints.removeAll()) {
          checkpoint.abandon();
        }
      }
    }
  }

  @VisibleForTesting
  static class UnboundedAmqpSource extends UnboundedSource<Message, AmqpCheckpointMark> {

    private final Read spec;

    public UnboundedAmqpSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public List<UnboundedAmqpSource> split(int desiredNumSplits, PipelineOptions pipelineOptions) {
      // amqp is a queue system, so, it's possible to have multiple concurrent sources, even if
      // they bind the listener
      List<UnboundedAmqpSource> sources = new ArrayList<>();
      for (int i = 0; i < Math.max(1, desiredNumSplits); ++i) {
        sources.add(new UnboundedAmqpSource(spec));
      }
      return sources;
    }

    @Override
    public UnboundedReader<Message> createReader(
        PipelineOptions pipelineOptions, @Nullable AmqpCheckpointMark checkpointMark) {
      if (checkpointMark != null) {
        // A runner resumes from a checkpoint only once it is committed, so its messages can be
        // acknowledged if the reader that took it lives in this JVM.
        checkpointMark.finalizeCheckpoint();
      }
      return new UnboundedAmqpReader(this, pipelineOptions);
    }

    @Override
    public Coder<Message> getOutputCoder() {
      return new AmqpMessageCoder();
    }

    @Override
    public Coder<AmqpCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(AmqpCheckpointMark.class);
    }
  }

  @VisibleForTesting
  static class UnboundedAmqpReader extends UnboundedSource.UnboundedReader<Message> {

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.standardSeconds(60);

    private final UnboundedAmqpSource source;
    private final PipelineOptions options;

    private @Nullable Messenger messenger;
    private @Nullable Message current;
    private @Nullable Instant currentTimestamp;
    private Instant watermark = new Instant(Long.MIN_VALUE);
    private final AmqpCheckpointMark.Preparer checkpointPreparer =
        new AmqpCheckpointMark.Preparer();
    private final AtomicInteger activeCheckpoints = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object closeLock = new Object();

    public UnboundedAmqpReader(UnboundedAmqpSource source, @Nullable PipelineOptions options) {
      this.source = source;
      this.options = options == null ? PipelineOptionsFactory.create() : options;
      this.current = null;
    }

    @Override
    public Instant getWatermark() {
      return watermark;
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (current == null || currentTimestamp == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public Message getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return checkpointPreparer.newCheckpoint(
          messenger, activeCheckpoints, this::maybeStopMessenger);
    }

    @Override
    public UnboundedAmqpSource getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      Read spec = source.spec;
      Messenger created = Messenger.Factory.create();
      created.start();
      for (String address : spec.addresses()) {
        created.subscribe(address);
      }
      messenger = created;
      return advance();
    }

    @Override
    public boolean advance() {
      Messenger currentMessenger = messenger;
      if (currentMessenger == null) {
        current = null;
        return false;
      }
      currentMessenger.recv();
      if (currentMessenger.incoming() <= 0) {
        current = null;
        return false;
      }
      Message message = currentMessenger.get();
      Tracker tracker = currentMessenger.incomingTracker();
      checkpointPreparer.add(tracker);
      currentTimestamp = new Instant(message.getCreationTime());
      watermark = currentTimestamp;
      current = message;
      return true;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void close() {
      closed.set(true);
      checkpointPreparer.discard();
      if (activeCheckpoints.get() == 0) {
        abandonPendingCheckpointsAndStopMessenger();
      } else {
        ScheduledExecutorService executorService =
            options.as(ExecutorOptions.class).getScheduledExecutorService();
        executorService.schedule(
            this::abandonPendingCheckpointsAndStopMessenger,
            DEFAULT_CLOSE_TIMEOUT.getMillis(),
            TimeUnit.MILLISECONDS);
      }
    }

    private void maybeStopMessenger() {
      if (closed.get() && activeCheckpoints.get() == 0) {
        abandonPendingCheckpointsAndStopMessenger();
      }
    }

    private void abandonPendingCheckpointsAndStopMessenger() {
      synchronized (closeLock) {
        checkpointPreparer.abandonPendingCheckpoints();
        if (messenger != null) {
          messenger.stop();
          messenger = null;
        }
      }
    }
  }

  /** A {@link PTransform} to send messages using AMQP 1.0 protocol. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Message>, PDone> {

    @Override
    public PDone expand(PCollection<Message> input) {
      input.apply(ParDo.of(new WriteFn()));
      return PDone.in(input.getPipeline());
    }

    private static class WriteFn extends DoFn<Message, Void> {

      private transient Messenger messenger;

      public WriteFn() {}

      @Setup
      public void setup() throws Exception {
        messenger = Messenger.Factory.create();
        messenger.start();
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        Message message = processContext.element();
        messenger.put(message);
        messenger.send();
      }

      @Teardown
      public void teardown() throws Exception {
        if (messenger != null) {
          messenger.stop();
        }
      }
    }
  }
}
