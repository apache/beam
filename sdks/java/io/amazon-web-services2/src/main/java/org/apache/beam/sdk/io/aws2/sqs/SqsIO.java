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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.buildClient;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
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

  /** @deprecated Use {@link #writeBatches()} for more configuration options. */
  @Deprecated
  public static Write write() {
    return new AutoValue_SqsIO_Write.Builder()
        .setClientConfiguration(ClientConfiguration.EMPTY)
        .build();
  }

  public static <T> WriteBatches<T> writeBatches() {
    return new AutoValue_SqsIO_WriteBatches.Builder<T>()
        .clientConfiguration(ClientConfiguration.EMPTY)
        .concurrentRequests(WriteBatches.DEFAULT_CONCURRENCY)
        .batchSize(WriteBatches.MAX_BATCH_SIZE)
        .batchTimeout(WriteBatches.DEFAULT_BATCH_TIMEOUT)
        .strictTimeouts(false)
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
   *
   * @deprecated superseded by {@link WriteBatches}
   */
  @AutoValue
  @Deprecated
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
      input.apply(
          SqsIO.<SendMessageRequest>writeBatches()
              .withBatchSize(1)
              .to(SendMessageRequest::queueUrl));
      return PDone.in(input.getPipeline());
    }
  }

  /**
   * A {@link PTransform} to send messages to SQS. See {@link SqsIO} for more information on usage
   * and configuration.
   */
  @AutoValue
  public abstract static class WriteBatches<T>
      extends PTransform<PCollection<T>, WriteBatches.Result> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteBatches.class);
    private static final int DEFAULT_CONCURRENCY = 5;
    private static final int MAX_BATCH_SIZE = 10;
    private static final Duration DEFAULT_BATCH_TIMEOUT = Duration.standardSeconds(3);

    abstract @Pure int concurrentRequests();

    abstract @Pure Duration batchTimeout();

    abstract @Pure boolean strictTimeouts();

    abstract @Pure int batchSize();

    abstract @Pure ClientConfiguration clientConfiguration();

    abstract @Pure @Nullable EntryMapperFn<T> entryMapper();

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

      abstract Builder<T> strictTimeouts(boolean strict);

      abstract Builder<T> batchSize(int batchSize);

      abstract Builder<T> clientConfiguration(ClientConfiguration config);

      abstract Builder<T> entryMapper(@Nullable EntryMapperFn<T> entryMapper);

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

    /**
     * Optional mapper to create a batch entry from a unique entry id and the input {@code T},
     * otherwise inferred from the schema.
     */
    public WriteBatches<T> withEntryMapper(EntryMapperFn<T> mapper) {
      return builder().entryMapper(mapper).build();
    }

    /**
     * Optional mapper to create a batch entry from the input {@code T} using a builder, otherwise
     * inferred from the schema.
     */
    public WriteBatches<T> withEntryMapper(EntryMapperFn.Builder<T> mapper) {
      return builder().entryMapper(mapper).build();
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
     * <p>By default timeouts will be checked upon arrival of records.
     */
    public WriteBatches<T> withBatchTimeout(Duration timeout) {
      return withBatchTimeout(timeout, false);
    }

    /**
     * The duration to accumulate records before timing out, default is 3 secs.
     *
     * <p>By default timeouts will be checked upon arrival of records. If using {@code strict}
     * enforcement, timeouts will be check by a separate thread.
     */
    public WriteBatches<T> withBatchTimeout(Duration timeout, boolean strict) {
      return builder().batchTimeout(timeout).strictTimeouts(strict).build();
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

    private EntryMapperFn<T> schemaEntryMapper(PCollection<T> input) {
      checkState(input.hasSchema(), "withEntryMapper is required if schema is not available");
      SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
      try {
        return new SchemaEntryMapper<>(
            input.getSchema(),
            registry.getSchema(SendMessageBatchRequestEntry.class),
            input.getToRowFunction(),
            registry.getFromRowFunction(SendMessageBatchRequestEntry.class));
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Result expand(PCollection<T> input) {
      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      ClientBuilderFactory.validate(awsOptions, clientConfiguration());
      EntryMapperFn<T> mapper = entryMapper() != null ? entryMapper() : schemaEntryMapper(input);
      input.apply(
          ParDo.of(
              new DoFn<T, Void>() {
                private @Nullable BatchHandler<T> handler = null;

                @Setup
                public void setup(PipelineOptions options) {
                  handler =
                      new BatchHandler<>(WriteBatches.this, mapper, options.as(AwsOptions.class));
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

    /**
     * Mapper to create a {@link SendMessageBatchRequestEntry} from a unique batch entry id and the
     * input {@code T}.
     */
    public interface EntryMapperFn<T>
        extends BiFunction<String, T, SendMessageBatchRequestEntry>, Serializable {

      /** A more convenient {@link EntryMapperFn} variant that already sets the entry id. */
      interface Builder<T>
          extends BiConsumer<SendMessageBatchRequestEntry.Builder, T>, EntryMapperFn<T> {
        @Override
        default SendMessageBatchRequestEntry apply(String entryId, T msg) {
          SendMessageBatchRequestEntry.Builder builder = SendMessageBatchRequestEntry.builder();
          accept(builder, msg);
          return builder.id(entryId).build();
        }
      }
    }

    @VisibleForTesting
    static class SchemaEntryMapper<T> implements EntryMapperFn<T> {
      private final SerializableFunction<T, Row> toRow;
      private final SerializableFunction<Row, SendMessageBatchRequestEntry> fromRow;
      private final Schema schema;
      private final int[] fieldMapping;

      SchemaEntryMapper(
          Schema sourceSchema,
          Schema targetSchema,
          SerializableFunction<T, Row> toRow,
          SerializableFunction<Row, SendMessageBatchRequestEntry> fromRow) {
        this.toRow = toRow;
        this.fromRow = fromRow;
        this.schema = targetSchema;
        this.fieldMapping = new int[targetSchema.getFieldCount()];

        Arrays.fill(fieldMapping, -1);

        List<String> ignored = Lists.newLinkedList();
        List<String> invalid = Lists.newLinkedList();

        for (int i = 0; i < sourceSchema.getFieldCount(); i++) {
          Field sourceField = sourceSchema.getField(i);
          if (targetSchema.hasField(sourceField.getName())) {
            int targetIdx = targetSchema.indexOf(sourceField.getName());
            // make sure field types match
            if (!sourceField.typesEqual(targetSchema.getField(targetIdx))) {
              invalid.add(sourceField.getName());
            }
            fieldMapping[targetIdx] = i;
          } else {
            ignored.add(sourceField.getName());
          }
        }
        checkState(
            ignored.size() < sourceSchema.getFieldCount(),
            "No fields matched, expected %s but got %s",
            schema.getFieldNames(),
            ignored);

        checkState(invalid.isEmpty(), "Detected incompatible types for input fields: {}", invalid);

        if (!ignored.isEmpty()) {
          LOG.warn("Ignoring unmatched input fields: {}", ignored);
        }
      }

      @Override
      public SendMessageBatchRequestEntry apply(String entryId, T input) {
        Row row = toRow.apply(input);
        Object[] values = new Object[fieldMapping.length];
        values[0] = entryId;
        for (int i = 0; i < values.length; i++) {
          if (fieldMapping[i] >= 0) {
            values[i] = row.getValue(fieldMapping[i]);
          }
        }
        return fromRow.apply(Row.withSchema(schema).attachValues(values));
      }
    }

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
      private static final int CHECKS_PER_TIMEOUT_PERIOD = 5;
      public static final int EXPIRATION_CHECK_TIMEOUT_SECS = 3;

      private final WriteBatches<T> spec;
      private final SqsAsyncClient sqs;
      private final Batches batches;
      private final EntryMapperFn<T> entryMapper;
      private final AsyncBatchWriteHandler<SendMessageBatchRequestEntry, BatchResultErrorEntry>
          handler;
      private final @Nullable ScheduledExecutorService scheduler;

      private @MonotonicNonNull ScheduledFuture<?> expirationCheck = null;

      BatchHandler(WriteBatches<T> spec, EntryMapperFn<T> entryMapper, AwsOptions options) {
        this.spec = spec;
        this.sqs = buildClient(options, SqsAsyncClient.builder(), spec.clientConfiguration());
        this.entryMapper = entryMapper;
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
        this.scheduler =
            spec.strictTimeouts() ? Executors.newSingleThreadScheduledExecutor() : null;
        if (spec.queueUrl() != null) {
          this.batches = new Single();
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
        if (scheduler != null && spec.strictTimeouts()) {
          long timeout = spec.batchTimeout().getMillis();
          long period = timeout / CHECKS_PER_TIMEOUT_PERIOD;
          expirationCheck =
              scheduler.scheduleWithFixedDelay(
                  () -> batches.submitExpired(false), timeout, period, MILLISECONDS);
        }
      }

      public void process(T msg) {
        SendMessageBatchRequestEntry entry = entryMapper.apply(batches.nextId(), msg);
        Batch batch = batches.getLocked(msg);
        batch.add(entry);
        if (batch.size() >= spec.batchSize() || batch.isExpired()) {
          submitEntries(batch, true);
        } else {
          checkState(batch.lock(false)); // unlock to continue writing to batch
        }

        if (scheduler == null) {
          // check for expired batches synchronously
          batches.submitExpired(true);
        }
      }

      /** Submit entries of a {@link Batch} to the async write handler. */
      private void submitEntries(Batch batch, boolean throwFailures) {
        try {
          handler.batchWrite(batch.queue, batch.getAndClose(), throwFailures);
        } catch (RuntimeException e) {
          throw e;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }

      public void finishBundle() throws Throwable {
        if (expirationCheck != null) {
          expirationCheck.cancel(false);
          while (true) {
            try {
              expirationCheck.get(EXPIRATION_CHECK_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
              LOG.warn("Waiting for timeout check to complete");
            } catch (CancellationException e) {
              break; // scheduled checks completed after cancellation
            }
          }
        }
        // safe to write remaining batches without risking to encounter locked ones
        checkState(batches.submitAll());
        handler.waitForCompletion();
      }

      @Override
      public void close() throws Exception {
        sqs.close();
        if (scheduler != null) {
          scheduler.shutdown();
        }
      }

      /**
       * Batch(es) of a single fixed or several dynamic queues.
       *
       * <p>A {@link Batch} can only ever be modified from the single runner thread.
       *
       * <p>In case of strict timeouts, a batch may be submitted to the write handler by periodic
       * expiration checks using a scheduler. Otherwise, and by default, this is done after
       * appending to a batch. {@link Batch#lock(boolean)} prevents concurrent access to a batch
       * between threads. Once a batch was locked by an expiration check, it must always be
       * submitted to the write handler.
       */
      @NotThreadSafe
      private abstract class Batches {
        private int nextId = 0; // only ever used from one "runner" thread

        abstract int maxBatches();

        /**
         * Next batch entry id is guaranteed to be unique for all open batches.
         *
         * <p>This method is not thread-safe and may only ever be called from the single runner
         * thread.
         */
        String nextId() {
          if (nextId >= (spec.batchSize() * maxBatches())) {
            nextId = 0;
          }
          return Integer.toString(nextId++);
        }

        /**
         * Get an existing or new locked batch to append new messages.
         *
         * <p>This method is not thread-safe and may only ever be called from a single runner
         * thread. If this encounters a locked batch, it assumes the {@link Batch} is currently
         * written to SQS and creates a new one.
         */
        abstract Batch getLocked(T record);

        /**
         * Submit all remaining batches (that can be locked) to the write handler.
         *
         * @return {@code true} if successful for all batches.
         */
        abstract boolean submitAll();

        /**
         * Submit all expired batches (that can be locked) to the write handler.
         *
         * <p>This is the only method that may be invoked from a thread other than the runner
         * thread.
         */
        abstract void submitExpired(boolean throwFailures);

        /**
         * Submit a batch to the write handler if it can be locked.
         *
         * @return {@code true} if successful (or closed).
         */
        protected boolean lockAndSubmit(Batch batch, boolean throwFailures) {
          if (batch.isClosed()) {
            return true; // nothing to submit
          } else if (batch.lock(true)) {
            submitEntries(batch, throwFailures);
            return true;
          }
          return false;
        }
      }

      /** Batch of a single, fixed queue. */
      @NotThreadSafe
      private class Single extends Batches {
        private @Nullable Batch batch;

        @Override
        int maxBatches() {
          return 1;
        }

        @Override
        Batch getLocked(T record) {
          if (batch == null || !batch.lock(true)) {
            batch = Batch.createLocked(checkStateNotNull(spec.queueUrl()), spec);
          }
          return checkStateNotNull(batch);
        }

        @Override
        boolean submitAll() {
          return batch == null || lockAndSubmit(batch, true);
        }

        @Override
        void submitExpired(boolean throwFailures) {
          if (batch != null && batch.isExpired()) {
            lockAndSubmit(checkStateNotNull(batch), throwFailures);
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
        private final AtomicBoolean submitExpiredRunning = new AtomicBoolean(false);
        private final AtomicReference<Instant> nextTimeout = new AtomicReference<>(Batch.NEVER);
        private final DynamicDestination<T> destination;

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
        boolean submitAll() {
          AtomicBoolean res = new AtomicBoolean(true);
          batches.values().forEach(batch -> res.compareAndSet(true, lockAndSubmit(batch, true)));
          batches.clear();
          nextTimeout.set(Batch.NEVER);
          return res.get();
        }

        private void updateNextTimeout(Batch batch) {
          Instant prev;
          do {
            prev = nextTimeout.get();
          } while (batch.expirationTime.isBefore(prev)
              && !nextTimeout.compareAndSet(prev, batch.expirationTime));
        }

        private void submitExpired(Batch batch, boolean throwFailures) {
          if (!batch.isClosed() && (!batch.isExpired() || !lockAndSubmit(batch, throwFailures))) {
            updateNextTimeout(batch);
          }
        }

        @Override
        void submitExpired(boolean throwFailures) {
          Instant timeout = nextTimeout.get();
          if (timeout.isBeforeNow()) {
            // prevent concurrent checks for expired batches
            if (submitExpiredRunning.compareAndSet(false, true)) {
              try {
                nextTimeout.set(Batch.NEVER);
                batches.values().forEach(b -> submitExpired(b, throwFailures));
              } catch (ConcurrentModificationException e) {
                // Can happen rarely when adding a new dynamic destination and is expected.
                // Reset old timeout to repeat check asap.
                nextTimeout.set(timeout);
              } finally {
                submitExpiredRunning.set(false);
              }
            }
          }
        }

        Batch createLocked(String queue) {
          Batch batch = Batch.createLocked(queue, spec);
          updateNextTimeout(batch);
          return batch;
        }
      }
    }

    /** Batch of entries of a queue. */
    @NotThreadSafe
    private abstract static class Batch {
      private static final Instant NEVER = Instant.ofEpochMilli(Long.MAX_VALUE);

      private final String queue;
      private final Instant expirationTime;
      private List<SendMessageBatchRequestEntry> entries;

      static Batch createLocked(String queue, SqsIO.WriteBatches<?> spec) {
        return spec.strictTimeouts()
            ? new BatchWithAtomicLock(queue, spec.batchSize(), spec.batchTimeout())
            : new BatchWithNoopLock(queue, spec.batchSize(), spec.batchTimeout());
      }

      /** A {@link Batch} with a noop lock that just rejects un/locking if closed. */
      private static class BatchWithNoopLock extends Batch {
        BatchWithNoopLock(String queue, int size, Duration timeout) {
          super(queue, size, timeout);
        }

        @Override
        boolean lock(boolean lock) {
          return !isClosed(); // always un/lock unless closed
        }
      }

      /** A {@link Batch} supporting atomic locking for concurrent usage. */
      private static class BatchWithAtomicLock extends Batch {
        private final AtomicBoolean locked = new AtomicBoolean(true); // always lock on creation

        BatchWithAtomicLock(String queue, int size, Duration timeout) {
          super(queue, size, timeout);
        }

        @Override
        boolean lock(boolean lock) {
          return !isClosed() && locked.compareAndSet(!lock, lock);
        }
      }

      private Batch(String queue, int size, Duration timeout) {
        this.queue = queue;
        this.entries = new ArrayList<>(size);
        this.expirationTime = Instant.now().plus(timeout);
      }

      /** Attempt to un/lock this batch, if closed this always fails. */
      abstract boolean lock(boolean lock);

      /**
       * Get and clear entries for submission to the write handler.
       *
       * <p>The batch must be locked and kept locked, it can't be modified anymore.
       */
      List<SendMessageBatchRequestEntry> getAndClose() {
        List<SendMessageBatchRequestEntry> res = entries;
        entries = EMPTY_LIST;
        return res;
      }

      /** Append entry (only use if locked!). */
      void add(SendMessageBatchRequestEntry entry) {
        entries.add(entry);
      }

      int size() {
        return entries.size();
      }

      boolean isExpired() {
        return expirationTime.isBeforeNow();
      }

      boolean isClosed() {
        return entries == EMPTY_LIST;
      }
    }
  }
}
