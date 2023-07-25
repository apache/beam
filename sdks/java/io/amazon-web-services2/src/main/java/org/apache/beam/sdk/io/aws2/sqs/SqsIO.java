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
package org.apache.beam.sdk.io.aws2.sqs;

import static java.util.Collections.EMPTY_LIST;
import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.buildClient;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.common.AsyncBatchWriteHandler;
import org.apache.beam.sdk.io.aws2.common.AsyncBatchWriteHandler.Stats;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;
import org.joda.time.Instant;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * IO to read (unbounded) from and write to <a href="https://aws.amazon.com/sqs/">SQS</a> queues.
 *
 * <h3>Reading from SQS</h3>
 *
 * <p>{@link Read} returns an unbounded {@link PCollection} of {@link SqsMessage}s. As minimum
 * configuration you have to provide the {@code queue url} to connect to using {@link
 * Read#withQueueUrl(String)}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<SqsMessage> output =
 *   pipeline.apply(SqsIO.read().withQueueUrl(queueUrl))
 * }</pre>
 *
 * <p>Note: Currently this source does not advance watermarks when no new messages are received.
 *
 * <h3>Writing to SQS</h3>
 *
 * <p>{@link Write} takes a {@link PCollection} of {@link SendMessageRequest}s as input. Each
 * request must contain the {@code queue url}. No further configuration is required.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<SendMessageRequest> data = ...;
 * data.apply(SqsIO.write())
 * }</pre>
 *
 * <h3>Configuration of AWS clients</h3>
 *
 * <p>AWS clients for all AWS IOs can be configured using {@link AwsOptions}, e.g. {@code
 * --awsRegion=us-west-1}. {@link AwsOptions} contain reasonable defaults based on default providers
 * for {@link Region} and {@link AwsCredentialsProvider}.
 *
 * <p>If you require more advanced configuration, you may change the {@link ClientBuilderFactory}
 * using {@link AwsOptions#setClientBuilderFactory(Class)}.
 *
 * <p>Configuration for a specific IO can be overwritten using {@code withClientConfiguration()},
 * which also allows to configure the retry behavior for the respective IO.
 *
 * <h4>Retries</h4>
 *
 * <p>Retries for failed requests can be configured using {@link
 * ClientConfiguration.Builder#retry(Consumer)} and are handled by the AWS SDK unless there's a
 * partial success (batch requests). The SDK uses a backoff strategy with equal jitter for computing
 * the delay before the next retry.
 *
 * <p><b>Note:</b> Once retries are exhausted the error is surfaced to the runner which <em>may</em>
 * then opt to retry the current partition in entirety or abort if the max number of retries of the
 * runner is reached.
 */
public class SqsIO {

  public static Read read() {
    return new AutoValue_SqsIO_Read.Builder()
        .setClientConfiguration(ClientConfiguration.EMPTY)
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  public static Write write() {
    return new AutoValue_SqsIO_Write.Builder()
        .setClientConfiguration(ClientConfiguration.EMPTY)
        .build();
  }

  public static <T> WriteBatches<T> writeBatches(WriteBatches.EntryBuilder<T> entryBuilder) {
    return new AutoValue_SqsIO_WriteBatches.Builder<T>()
        .clientConfiguration(ClientConfiguration.EMPTY)
        .concurrentRequests(WriteBatches.DEFAULT_CONCURRENCY)
        .batchSize(WriteBatches.MAX_BATCH_SIZE)
        .batchTimeout(WriteBatches.DEFAULT_BATCH_TIMEOUT)
        .entryBuilder(entryBuilder)
        .build();
  }

  private SqsIO() {}

  /**
   * A {@link PTransform} to read/receive messages from SQS. See {@link SqsIO} for more information
   * on usage and configuration.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<SqsMessage>> {

    abstract ClientConfiguration clientConfiguration();

    abstract @Nullable String queueUrl();

    abstract long maxNumRecords();

    abstract @Pure @Nullable Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setClientConfiguration(ClientConfiguration config);

      abstract Builder setQueueUrl(String queueUrl);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
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

    /** Define the queueUrl used by the {@link Read} to receive messages from SQS. */
    public Read withQueueUrl(String queueUrl) {
      checkArgument(queueUrl != null, "queueUrl can not be null");
      checkArgument(!queueUrl.isEmpty(), "queueUrl can not be empty");
      return builder().setQueueUrl(queueUrl).build();
    }

    /** Configuration of SQS client. */
    public Read withClientConfiguration(ClientConfiguration config) {
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return builder().setClientConfiguration(config).build();
    }

    @Override
    @SuppressWarnings({"nullness"})
    public PCollection<SqsMessage> expand(PBegin input) {
      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      ClientBuilderFactory.validate(awsOptions, clientConfiguration());

      org.apache.beam.sdk.io.Read.Unbounded<SqsMessage> unbounded =
          org.apache.beam.sdk.io.Read.from(new SqsUnboundedSource(this));

      PTransform<PBegin, PCollection<SqsMessage>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }
  }
  /**
   * A {@link PTransform} to send messages to SQS. See {@link SqsIO} for more information on usage
   * and configuration.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<SendMessageRequest>, PDone> {

    abstract @Pure ClientConfiguration getClientConfiguration();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setClientConfiguration(ClientConfiguration config);

      abstract Write build();
    }

    /** Configuration of SQS client. */
    public Write withClientConfiguration(ClientConfiguration config) {
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return builder().setClientConfiguration(config).build();
    }

    @Override
    public PDone expand(PCollection<SendMessageRequest> input) {
      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      ClientBuilderFactory.validate(awsOptions, getClientConfiguration());

      input.apply(ParDo.of(new SqsWriteFn(this)));
      return PDone.in(input.getPipeline());
    }
  }

  private static class SqsWriteFn extends DoFn<SendMessageRequest, Void> {
    private final Write spec;
    private transient @MonotonicNonNull SqsClient sqs = null;

    SqsWriteFn(Write write) {
      this.spec = write;
    }

    @Setup
    public void setup(PipelineOptions options) throws Exception {
      AwsOptions awsOpts = options.as(AwsOptions.class);
      sqs =
          ClientBuilderFactory.buildClient(
              awsOpts, SqsClient.builder(), spec.getClientConfiguration());
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      if (sqs == null) {
        throw new IllegalStateException("No SQS client");
      }
      sqs.sendMessage(processContext.element());
    }
  }

  /**
   * A {@link PTransform} to send messages to SQS. See {@link SqsIO} for more information on usage
   * and configuration.
   */
  @AutoValue
  public abstract static class WriteBatches<T>
      extends PTransform<PCollection<T>, WriteBatches.Result> {
    private static final int DEFAULT_CONCURRENCY = 5;
    private static final int MAX_BATCH_SIZE = 10;
    private static final Duration DEFAULT_BATCH_TIMEOUT = Duration.standardSeconds(3);

    abstract @Pure int concurrentRequests();

    abstract @Pure Duration batchTimeout();

    abstract @Pure int batchSize();

    abstract @Pure ClientConfiguration clientConfiguration();

    abstract @Pure EntryBuilder<T> entryBuilder();

    abstract @Pure @Nullable DynamicDestination<T> dynamicDestination();

    abstract @Pure @Nullable String queueUrl();

    abstract Builder<T> builder();

    public interface DynamicDestination<T> extends Serializable {
      String queueUrl(T message);
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> concurrentRequests(int concurrentRequests);

      abstract Builder<T> batchTimeout(Duration duration);

      abstract Builder<T> batchSize(int batchSize);

      abstract Builder<T> clientConfiguration(ClientConfiguration config);

      abstract Builder<T> entryBuilder(EntryBuilder<T> entryBuilder);

      abstract Builder<T> dynamicDestination(@Nullable DynamicDestination<T> destination);

      abstract Builder<T> queueUrl(@Nullable String queueUrl);

      abstract WriteBatches<T> build();
    }

    /** Configuration of SQS client. */
    public WriteBatches<T> withClientConfiguration(ClientConfiguration config) {
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return builder().clientConfiguration(config).build();
    }

    /** Max number of concurrent batch write requests per bundle, default is {@code 5}. */
    public WriteBatches<T> withConcurrentRequests(int concurrentRequests) {
      checkArgument(concurrentRequests > 0, "concurrentRequests must be > 0");
      return builder().concurrentRequests(concurrentRequests).build();
    }

    /** The batch size to use, default (and AWS limit) is {@code 10}. */
    public WriteBatches<T> withBatchSize(int batchSize) {
      checkArgument(
          batchSize > 0 && batchSize <= MAX_BATCH_SIZE,
          "Maximum allowed batch size is " + MAX_BATCH_SIZE);
      return builder().batchSize(batchSize).build();
    }

    /**
     * The duration to accumulate records before timing out, default is 3 secs.
     *
     * <p>Timeouts will be checked upon arrival of new messages.
     */
    public WriteBatches<T> withBatchTimeout(Duration timeout) {
      return builder().batchTimeout(timeout).build();
    }

    /** Dynamic record based destination to write to. */
    public WriteBatches<T> to(DynamicDestination<T> destination) {
      checkArgument(destination != null, "DynamicDestination cannot be null");
      return builder().queueUrl(null).dynamicDestination(destination).build();
    }

    /** Queue url to write to. */
    public WriteBatches<T> to(String queueUrl) {
      checkArgument(queueUrl != null, "queueUrl cannot be null");
      return builder().dynamicDestination(null).queueUrl(queueUrl).build();
    }

    @Override
    public Result expand(PCollection<T> input) {
      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      ClientBuilderFactory.validate(awsOptions, clientConfiguration());

      input.apply(
          ParDo.of(
              new DoFn<T, Void>() {
                private @Nullable BatchHandler<T> handler = null;

                @Setup
                public void setup(PipelineOptions options) {
                  handler = new BatchHandler<>(WriteBatches.this, options.as(AwsOptions.class));
                }

                @StartBundle
                public void startBundle() {
                  handler().startBundle();
                }

                @ProcessElement
                public void processElement(ProcessContext cxt) throws Throwable {
                  handler().process(cxt.element());
                }

                @FinishBundle
                public void finishBundle() throws Throwable {
                  handler().finishBundle();
                }

                @Teardown
                public void teardown() throws Exception {
                  if (handler != null) {
                    handler.close();
                    handler = null;
                  }
                }

                private BatchHandler<T> handler() {
                  return checkStateNotNull(handler, "SQS handler is null");
                }
              }));
      return new Result(input.getPipeline());
    }

    /** Batch entry builder. */
    public interface EntryBuilder<T>
        extends BiConsumer<SendMessageBatchRequestEntry.Builder, T>, Serializable {}

    /** Result of {@link #writeBatches}. */
    public static class Result implements POutput {
      private final Pipeline pipeline;

      private Result(Pipeline pipeline) {
        this.pipeline = pipeline;
      }

      @Override
      public Pipeline getPipeline() {
        return pipeline;
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of();
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    private static class BatchHandler<T> implements AutoCloseable {
      private final WriteBatches<T> spec;
      private final SqsAsyncClient sqs;
      private final Batches batches;
      private final AsyncBatchWriteHandler<SendMessageBatchRequestEntry, BatchResultErrorEntry>
          handler;

      BatchHandler(WriteBatches<T> spec, AwsOptions options) {
        this.spec = spec;
        this.sqs = buildClient(options, SqsAsyncClient.builder(), spec.clientConfiguration());
        this.handler =
            AsyncBatchWriteHandler.byId(
                spec.concurrentRequests(),
                spec.batchSize(),
                spec.clientConfiguration().retry(),
                Stats.NONE,
                (queue, records) -> sendMessageBatch(sqs, queue, records),
                error -> error.code(),
                record -> record.id(),
                error -> error.id());
        if (spec.queueUrl() != null) {
          this.batches = new Single(spec.queueUrl());
        } else if (spec.dynamicDestination() != null) {
          this.batches = new Dynamic(spec.dynamicDestination());
        } else {
          throw new IllegalStateException("to(queueUrl) or to(dynamicDestination) is required");
        }
      }

      private static CompletableFuture<List<BatchResultErrorEntry>> sendMessageBatch(
          SqsAsyncClient sqs, String queue, List<SendMessageBatchRequestEntry> records) {
        SendMessageBatchRequest request =
            SendMessageBatchRequest.builder().queueUrl(queue).entries(records).build();
        return sqs.sendMessageBatch(request).thenApply(resp -> resp.failed());
      }

      public void startBundle() {
        handler.reset();
      }

      public void process(T msg) {
        SendMessageBatchRequestEntry.Builder builder = SendMessageBatchRequestEntry.builder();
        spec.entryBuilder().accept(builder, msg);
        SendMessageBatchRequestEntry entry = builder.id(batches.nextId()).build();

        Batch batch = batches.getLocked(msg);
        batch.add(entry);
        if (batch.size() >= spec.batchSize() || batch.isExpired()) {
          writeEntries(batch, true);
        } else {
          checkState(batch.lock(false)); // unlock to continue writing to batch
        }

        // check timeouts synchronously on arrival of new messages
        batches.writeExpired(true);
      }

      private void writeEntries(Batch batch, boolean throwPendingFailures) {
        try {
          handler.batchWrite(batch.queue, batch.getAndClear(), throwPendingFailures);
        } catch (RuntimeException e) {
          throw e;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }

      public void finishBundle() throws Throwable {
        batches.writeAll();
        handler.waitForCompletion();
      }

      @Override
      public void close() throws Exception {
        sqs.close();
      }

      /**
       * Batch(es) of a single fixed or several dynamic queues.
       *
       * <p>{@link #getLocked} is meant to support atomic writes from multiple threads if using an
       * appropriate thread-safe implementation. This is necessary to later support strict timeouts
       * (see below).
       *
       * <p>For simplicity, check for expired messages after appending to a batch. For strict
       * enforcement of timeouts, {@link #writeExpired} would have to be periodically called using a
       * scheduler and requires also a thread-safe impl of {@link Batch#lock(boolean)}.
       */
      private abstract class Batches {
        private int nextId = 0; // only ever used from one "runner" thread

        abstract int maxBatches();

        /** Next batch entry id is guaranteed to be unique for all open batches. */
        String nextId() {
          if (nextId >= (spec.batchSize() * maxBatches())) {
            nextId = 0;
          }
          return Integer.toString(nextId++);
        }

        /** Get existing or new locked batch that can be written to. */
        abstract Batch getLocked(T record);

        /** Write all remaining batches (that can be locked). */
        abstract void writeAll();

        /** Write all expired batches (that can be locked). */
        abstract void writeExpired(boolean throwPendingFailures);

        /** Create a new locked batch that is ready for writing. */
        Batch createLocked(String queue) {
          return new Batch(queue, spec.batchSize(), spec.batchTimeout());
        }

        /** Write a batch if it can be locked. */
        protected boolean writeLocked(Batch batch, boolean throwPendingFailures) {
          if (batch.lock(true)) {
            writeEntries(batch, throwPendingFailures);
            return true;
          }
          return false;
        }
      }

      /** Batch of a single, fixed queue. */
      @NotThreadSafe
      private class Single extends Batches {
        private Batch batch;

        Single(String queue) {
          this.batch = new Batch(queue, EMPTY_LIST, Batch.NEVER); // locked
        }

        @Override
        int maxBatches() {
          return 1;
        }

        @Override
        Batch getLocked(T record) {
          return batch.lock(true) ? batch : (batch = createLocked(batch.queue));
        }

        @Override
        void writeAll() {
          writeLocked(batch, true);
        }

        @Override
        void writeExpired(boolean throwPendingFailures) {
          if (batch.isExpired()) {
            writeLocked(batch, throwPendingFailures);
          }
        }
      }

      /** Batches of one or several dynamic queues. */
      @NotThreadSafe
      private class Dynamic extends Batches {
        @SuppressWarnings("method.invocation") // necessary dependencies are initialized
        private final BiFunction<@NonNull String, @Nullable Batch, Batch> getLocked =
            (queue, batch) -> batch != null && batch.lock(true) ? batch : createLocked(queue);

        private final Map<@NonNull String, Batch> batches = new HashMap<>();
        private final DynamicDestination<T> destination;
        private Instant nextTimeout = Batch.NEVER;

        Dynamic(DynamicDestination<T> destination) {
          this.destination = destination;
        }

        @Override
        int maxBatches() {
          return batches.size() + 1; // next record and id might belong to new batch
        }

        @Override
        Batch getLocked(T record) {
          return batches.compute(destination.queueUrl(record), getLocked);
        }

        @Override
        void writeAll() {
          batches.values().forEach(batch -> writeLocked(batch, true));
          batches.clear();
          nextTimeout = Batch.NEVER;
        }

        private void writeExpired(Batch batch) {
          if (!batch.isExpired() || !writeLocked(batch, true)) {
            // find next timeout for remaining, unwritten batches
            if (batch.timeout.isBefore(nextTimeout)) {
              nextTimeout = batch.timeout;
            }
          }
        }

        @Override
        void writeExpired(boolean throwPendingFailures) {
          if (nextTimeout.isBeforeNow()) {
            nextTimeout = Batch.NEVER;
            batches.values().forEach(this::writeExpired);
          }
        }

        @Override
        Batch createLocked(String queue) {
          Batch batch = super.createLocked(queue);
          if (batch.timeout.isBefore(nextTimeout)) {
            nextTimeout = batch.timeout;
          }
          return batch;
        }
      }
    }

    /**
     * Batch of entries of a queue.
     *
     * <p>Overwrite {@link #lock} with a thread-safe implementation to support concurrent usage.
     */
    @NotThreadSafe
    private static final class Batch {
      private static final Instant NEVER = Instant.ofEpochMilli(Long.MAX_VALUE);
      private final String queue;
      private Instant timeout;
      private List<SendMessageBatchRequestEntry> entries;

      Batch(String queue, int size, Duration bufferedTime) {
        this(queue, new ArrayList<>(size), Instant.now().plus(bufferedTime));
      }

      Batch(String queue, List<SendMessageBatchRequestEntry> entries, Instant timeout) {
        this.queue = queue;
        this.entries = entries;
        this.timeout = timeout;
      }

      /** Attempt to un/lock this batch and return if successful. */
      boolean lock(boolean lock) {
        // thread unsafe dummy impl that rejects locking batches after getAndClear
        return !NEVER.equals(timeout) || !lock;
      }

      /** Get and clear entries for writing. */
      List<SendMessageBatchRequestEntry> getAndClear() {
        List<SendMessageBatchRequestEntry> res = entries;
        entries = EMPTY_LIST;
        timeout = NEVER;
        return res;
      }

      /** Add entry to this batch. */
      void add(SendMessageBatchRequestEntry entry) {
        entries.add(entry);
      }

      int size() {
        return entries.size();
      }

      boolean isExpired() {
        return timeout.isBeforeNow();
      }
    }
  }
}
