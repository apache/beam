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
package org.apache.beam.sdk.io.gcp.firestore;

import static java.util.Objects.requireNonNull;

import com.google.cloud.firestore.v1.stub.FirestoreStub;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.DatabaseRootName;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.ExplicitlyWindowedFirestoreDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.FailedWritesException;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteFailure;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteSuccessSummary;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.FlushBuffer;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of {@link org.apache.beam.sdk.transforms.DoFn DoFn}s for each of the supported write
 * RPC methods from the Cloud Firestore V1 API.
 */
final class FirestoreV1WriteFn {

  static final class BatchWriteFnWithSummary extends BaseBatchWriteFn<WriteSuccessSummary> {
    BatchWriteFnWithSummary(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        CounterFactory counterFactory) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, counterFactory);
    }

    @Override
    void handleWriteFailures(
        ContextAdapter<WriteSuccessSummary> context,
        Instant timestamp,
        List<KV<WriteFailure, BoundedWindow>> writeFailures,
        Runnable logMessage) {
      throw new FailedWritesException(
          writeFailures.stream().map(KV::getKey).collect(Collectors.toList()));
    }

    @Override
    void handleWriteSummary(
        ContextAdapter<WriteSuccessSummary> context,
        Instant timestamp,
        KV<WriteSuccessSummary, BoundedWindow> tuple,
        Runnable logMessage) {
      logMessage.run();
      context.output(tuple.getKey(), timestamp, tuple.getValue());
    }
  }

  static final class BatchWriteFnWithDeadLetterQueue extends BaseBatchWriteFn<WriteFailure> {
    BatchWriteFnWithDeadLetterQueue(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        CounterFactory counterFactory) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, counterFactory);
    }

    @Override
    void handleWriteFailures(
        ContextAdapter<WriteFailure> context,
        Instant timestamp,
        List<KV<WriteFailure, BoundedWindow>> writeFailures,
        Runnable logMessage) {
      logMessage.run();
      for (KV<WriteFailure, BoundedWindow> kv : writeFailures) {
        context.output(kv.getKey(), timestamp, kv.getValue());
      }
    }

    @Override
    void handleWriteSummary(
        ContextAdapter<WriteFailure> context,
        Instant timestamp,
        KV<WriteSuccessSummary, BoundedWindow> tuple,
        Runnable logMessage) {
      logMessage.run();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link BatchWriteRequest}s.
   *
   * <p>Writes will be enqueued to be sent at a potentially later time when more writes are
   * available. This Fn attempts to maximize throughput while maintaining a high request success
   * rate.
   *
   * <p>All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  abstract static class BaseBatchWriteFn<OutT> extends ExplicitlyWindowedFirestoreDoFn<Write, OutT>
      implements HasRpcAttemptContext {
    private static final Logger LOG =
        LoggerFactory.getLogger(
            FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.BatchWrite.getNamespace());
    private final JodaClock clock;
    private final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
    private final RpcQosOptions rpcQosOptions;
    private final CounterFactory counterFactory;
    private final V1FnRpcAttemptContext rpcAttemptContext;

    // transient running state information, not important to any possible checkpointing
    //  worker scoped state
    private transient RpcQos rpcQos;
    private transient Counter writesSuccessful;
    private transient Counter writesFailedRetryable;
    private transient Counter writesFailedNonRetryable;
    //  bundle scoped state
    private transient FirestoreStub firestoreStub;
    private transient DatabaseRootName databaseRootName;

    @VisibleForTesting
    transient Queue<@NonNull WriteElement> writes = new PriorityQueue<>(WriteElement.COMPARATOR);

    @VisibleForTesting transient int queueNextEntryPriority = 0;

    @SuppressWarnings(
        "initialization.fields.uninitialized") // allow transient fields to be managed by component
    // lifecycle
    BaseBatchWriteFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        CounterFactory counterFactory) {
      this.clock = clock;
      this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
      this.rpcQosOptions = rpcQosOptions;
      this.counterFactory = counterFactory;
      this.rpcAttemptContext = V1FnRpcAttemptContext.BatchWrite;
    }

    @Override
    public Context getRpcAttemptContext() {
      return rpcAttemptContext;
    }

    @Override
    public final void populateDisplayData(DisplayData.Builder builder) {
      builder.include("rpcQosOptions", rpcQosOptions);
    }

    @Override
    public void setup() {
      rpcQos = firestoreStatefulComponentFactory.getRpcQos(rpcQosOptions);
      writes = new PriorityQueue<>(WriteElement.COMPARATOR);

      String namespace = rpcAttemptContext.getNamespace();
      writesSuccessful = counterFactory.get(namespace, "writes_successful");
      writesFailedRetryable = counterFactory.get(namespace, "writes_failed_retryable");
      writesFailedNonRetryable = counterFactory.get(namespace, "writes_failed_non-retryable");
    }

    @Override
    public final void startBundle(StartBundleContext c) {
      String project = c.getPipelineOptions().as(FirestoreOptions.class).getFirestoreProject();
      if (project == null) {
        project = c.getPipelineOptions().as(GcpOptions.class).getProject();
      }
      String databaseId = c.getPipelineOptions().as(FirestoreOptions.class).getFirestoreDb();
      databaseRootName =
          DatabaseRootName.of(
              requireNonNull(
                  project,
                  "project must be defined on FirestoreOptions or GcpOptions of PipelineOptions"),
              requireNonNull(
                  databaseId,
                  "firestoreDb must be defined on FirestoreOptions of PipelineOptions"));
      firestoreStub = firestoreStatefulComponentFactory.getFirestoreStub(c.getPipelineOptions());
    }

    /**
     * For each element extract and enqueue all writes from the commit. Then potentially flush any
     * previously and currently enqueued writes.
     *
     * <p>In order for writes to be enqueued the value of {@link BatchWriteRequest#getDatabase()}
     * must match exactly with the database name this instance is configured for via the provided
     * {@link org.apache.beam.sdk.options.PipelineOptions PipelineOptions}
     *
     * <p>{@inheritDoc}
     */
    @Override
    public void processElement(ProcessContext context, BoundedWindow window) throws Exception {
      @SuppressWarnings(
          "nullness") // error checker is configured to treat any method not explicitly annotated as
      // @Nullable as non-null, this includes Objects.requireNonNull
      Write write = requireNonNull(context.element(), "context.element() must be non null");
      ProcessContextAdapter<OutT> contextAdapter = new ProcessContextAdapter<>(context);
      int serializedSize = write.getSerializedSize();
      boolean tooLarge = rpcQos.bytesOverLimit(serializedSize);
      if (tooLarge) {
        String message =
            String.format(
                "%s for document '%s' larger than configured max allowed bytes per batch",
                getWriteType(write), getName(write));
        handleWriteFailures(
            contextAdapter,
            clock.instant(),
            ImmutableList.of(
                KV.of(
                    new WriteFailure(
                        write,
                        WriteResult.newBuilder().build(),
                        Status.newBuilder()
                            .setCode(Code.INVALID_ARGUMENT.getNumber())
                            .setMessage(message)
                            .build()),
                    window)),
            () -> LOG.info(message));
      } else {
        writes.offer(new WriteElement(queueNextEntryPriority++, write, window));
        flushBatch(/* finishingBundle */ false, contextAdapter);
      }
    }

    /**
     * Attempt to flush any outstanding enqueued writes before cleaning up any bundle related state.
     * {@inheritDoc}
     */
    @SuppressWarnings("nullness") // allow clearing transient fields
    @Override
    public void finishBundle(FinishBundleContext context) throws Exception {
      try {
        flushBatch(/* finishingBundle */ true, new FinishBundleContextAdapter<>(context));
      } finally {
        databaseRootName = null;
        firestoreStub.close();
      }
    }

    /**
     * Possibly flush enqueued writes to Firestore.
     *
     * <p>This flush attempts to maximize throughput and success rate of RPCs. When a flush should
     * happen and how many writes are included is determined and managed by the {@link RpcQos}
     * instance of this class.
     *
     * @param finishingBundle A boolean specifying if this call is from {@link
     *     #finishBundle(DoFn.FinishBundleContext)}. If {@code true}, this method will not return
     *     until a terminal state (success, attempts exhausted) for all enqueued writes is reached.
     *     If {@code false} and the batch buffer is not full, this method will yield until another
     *     element is added.
     * @throws InterruptedException If the current thread is interrupted at anytime, such as while
     *     waiting for the next attempt
     * @see RpcQos
     * @see RpcQos.RpcWriteAttempt
     * @see BackOffUtils#next(org.apache.beam.sdk.util.Sleeper, org.apache.beam.sdk.util.BackOff)
     */
    private void flushBatch(boolean finishingBundle, ContextAdapter<OutT> context)
        throws InterruptedException {
      while (!writes.isEmpty()) {
        RpcWriteAttempt attempt = rpcQos.newWriteAttempt(getRpcAttemptContext());
        Instant begin = clock.instant();
        if (!attempt.awaitSafeToProceed(begin)) {
          continue;
        }

        FlushBuffer<WriteElement> flushBuffer = getFlushBuffer(attempt, begin);
        if (flushBuffer.isFull() || (finishingBundle && flushBuffer.isNonEmpty())) {
          DoFlushStatus flushStatus = doFlush(attempt, flushBuffer, context);
          if (flushStatus == DoFlushStatus.ONE_OR_MORE_FAILURES && !finishingBundle) {
            break;
          }
        } else {
          // since we're not going to perform a flush, we need to return the writes that were
          // preemptively removed
          flushBuffer.forEach(writes::offer);
          if (!finishingBundle) {
            // we're not on the final flush, so yield until more elements are delivered or
            // finishingBundle is true
            return;
          }
        }
      }
      if (writes.isEmpty()) {
        // now that the queue has been emptied reset our priority back to 0 to try and ensure
        // we won't run into overflow issues if a worker runs for a long time and processes
        // many writes.
        queueNextEntryPriority = 0;
      }
    }

    private FlushBuffer<WriteElement> getFlushBuffer(RpcWriteAttempt attempt, Instant start) {
      FlushBuffer<WriteElement> buffer = attempt.newFlushBuffer(start);

      WriteElement peek;
      while ((peek = writes.peek()) != null) {
        if (buffer.offer(peek)) {
          writes.poll();
        } else {
          break;
        }
      }
      return buffer;
    }

    private BatchWriteRequest getBatchWriteRequest(FlushBuffer<WriteElement> flushBuffer) {
      BatchWriteRequest.Builder commitBuilder =
          BatchWriteRequest.newBuilder().setDatabase(databaseRootName.toString());
      for (WriteElement element : flushBuffer) {
        commitBuilder.addWrites(element.getValue());
      }
      return commitBuilder.build();
    }

    private DoFlushStatus doFlush(
        RpcWriteAttempt attempt,
        FlushBuffer<WriteElement> flushBuffer,
        ContextAdapter<OutT> context)
        throws InterruptedException {
      int writesCount = flushBuffer.getBufferedElementsCount();
      long bytes = flushBuffer.getBufferedElementsBytes();
      BatchWriteRequest request = getBatchWriteRequest(flushBuffer);
      // This is our retry loop
      //    If an error is encountered and is retryable, continue will start the loop over again
      //    If an error is encountered and is not retryable, the error will be thrown and the loop
      // will end
      //    If no error is encountered the responses WriteResults will be inspected before breaking
      // the loop
      while (true) {
        Instant start = clock.instant();
        LOG.debug(
            "Sending BatchWrite request with {} writes totalling {} bytes", writesCount, bytes);
        Instant end;
        BatchWriteResponse response;
        try {
          attempt.recordRequestStart(start, writesCount);
          response = firestoreStub.batchWriteCallable().call(request);
          end = clock.instant();
          attempt.recordRequestSuccessful(end);
        } catch (RuntimeException exception) {
          end = clock.instant();
          String exceptionMessage = exception.getMessage();
          LOG.warn(
              "Sending BatchWrite request with {} writes totalling {} bytes failed due to error: {}",
              writesCount,
              bytes,
              exceptionMessage != null ? exceptionMessage : exception.getClass().getName());
          attempt.recordRequestFailed(end);
          attempt.recordWriteCounts(end, 0, writesCount);
          flushBuffer.forEach(writes::offer);
          attempt.checkCanRetry(end, exception);
          continue;
        }

        long elapsedMillis = end.minus(Duration.millis(start.getMillis())).getMillis();

        int okCount = 0;
        long okBytes = 0L;
        BoundedWindow okWindow = null;
        List<KV<WriteFailure, BoundedWindow>> nonRetryableWrites = new ArrayList<>();

        List<WriteResult> writeResultList = response.getWriteResultsList();
        List<Status> statusList = response.getStatusList();

        Iterator<WriteElement> iterator = flushBuffer.iterator();
        for (int i = 0; iterator.hasNext() && i < statusList.size(); i++) {
          WriteElement writeElement = iterator.next();
          Status writeStatus = statusList.get(i);
          Code code = Code.forNumber(writeStatus.getCode());

          if (code == Code.OK) {
            okCount++;
            okBytes += writeElement.getSerializedSize();
            okWindow = writeElement.window;
          } else {
            if (attempt.isCodeRetryable(code)) {
              writes.offer(writeElement);
            } else {
              nonRetryableWrites.add(
                  KV.of(
                      new WriteFailure(
                          writeElement.getValue(), writeResultList.get(i), writeStatus),
                      writeElement.window));
            }
          }
        }

        int nonRetryableCount = nonRetryableWrites.size();
        int retryableCount = writesCount - okCount - nonRetryableCount;

        writesSuccessful.inc(okCount);
        writesFailedRetryable.inc(retryableCount);
        writesFailedNonRetryable.inc(nonRetryableCount);

        attempt.recordWriteCounts(end, okCount, nonRetryableCount + retryableCount);
        if (okCount == writesCount) {
          handleWriteSummary(
              context,
              end,
              KV.of(new WriteSuccessSummary(okCount, okBytes), coerceNonNull(okWindow)),
              () ->
                  LOG.debug(
                      "Sending BatchWrite request with {} writes totalling {} bytes was completely applied in {}ms",
                      writesCount,
                      bytes,
                      elapsedMillis));
          attempt.completeSuccess();
          return DoFlushStatus.OK;
        } else {
          if (nonRetryableCount > 0) {
            int finalOkCount = okCount;
            handleWriteFailures(
                context,
                end,
                ImmutableList.copyOf(nonRetryableWrites),
                () ->
                    LOG.warn(
                        "Sending BatchWrite request with {} writes totalling {} bytes was incompletely applied in {}ms ({} ok, {} retryable, {} non-retryable)",
                        writesCount,
                        bytes,
                        elapsedMillis,
                        finalOkCount,
                        retryableCount,
                        nonRetryableCount));
          } else if (retryableCount > 0) {
            int finalOkCount = okCount;
            Runnable logMessage =
                () ->
                    LOG.debug(
                        "Sending BatchWrite request with {} writes totalling {} bytes was incompletely applied in {}ms ({} ok, {} retryable)",
                        writesCount,
                        bytes,
                        elapsedMillis,
                        finalOkCount,
                        retryableCount);
            if (okCount > 0) {
              handleWriteSummary(
                  context,
                  end,
                  KV.of(new WriteSuccessSummary(okCount, okBytes), coerceNonNull(okWindow)),
                  logMessage);
            } else {
              logMessage.run();
            }
          }
          return DoFlushStatus.ONE_OR_MORE_FAILURES;
        }
      }
    }

    /**
     * Window values are part of the WriteElement which is used to full the flushBuffer and
     * accessible after the response is returned. Our FlushBuffer is ensured to be non-empty before
     * passed to this method, so the loop above will always iterate at least once and by virtue of
     * this method only being when at least one ok status OK is present successfulWindow will be
     * non-null.
     *
     * <p>This method is here to prove to error prone that the value of successfulWindow we are
     * passing along is in fact non-null.
     */
    private static BoundedWindow coerceNonNull(@Nullable BoundedWindow successfulWindow) {
      if (successfulWindow == null) {
        throw new IllegalStateException("Unable to locate window for successful request");
      }
      return successfulWindow;
    }

    private enum DoFlushStatus {
      OK,
      ONE_OR_MORE_FAILURES
    }

    abstract void handleWriteFailures(
        ContextAdapter<OutT> context,
        Instant timestamp,
        List<KV<WriteFailure, BoundedWindow>> writeFailures,
        Runnable logMessage);

    abstract void handleWriteSummary(
        ContextAdapter<OutT> context,
        Instant timestamp,
        KV<WriteSuccessSummary, BoundedWindow> tuple,
        Runnable logMessage);

    private static String getWriteType(Write w) {
      if (w.hasUpdate()) {
        return "UPDATE";
      } else if (w.hasTransform()) {
        return "TRANSFORM";
      } else {
        return "DELETE";
      }
    }

    private static String getName(Write w) {
      if (w.hasUpdate()) {
        return w.getUpdate().getName();
      } else if (w.hasTransform()) {
        return w.getTransform().getDocument();
      } else {
        return w.getDelete();
      }
    }

    /**
     * Adapter interface which provides a common parent for {@link ProcessContext} and {@link
     * FinishBundleContext} so that we are able to use a single common invocation to output from.
     */
    interface ContextAdapter<T> {
      void output(T t, Instant timestamp, BoundedWindow window);
    }

    private static final class ProcessContextAdapter<T> implements ContextAdapter<T> {
      private final DoFn<Write, T>.ProcessContext context;

      private ProcessContextAdapter(DoFn<Write, T>.ProcessContext context) {
        this.context = context;
      }

      @Override
      public void output(T t, Instant timestamp, BoundedWindow window) {
        context.outputWithTimestamp(t, timestamp);
      }
    }

    private static final class FinishBundleContextAdapter<T> implements ContextAdapter<T> {
      private final DoFn<Write, T>.FinishBundleContext context;

      private FinishBundleContextAdapter(DoFn<Write, T>.FinishBundleContext context) {
        this.context = context;
      }

      @Override
      public void output(T t, Instant timestamp, BoundedWindow window) {
        context.output(t, timestamp, window);
      }
    }
  }

  static final class WriteElement implements Element<Write> {

    private static final Comparator<WriteElement> COMPARATOR =
        Comparator.comparing(WriteElement::getQueuePosition);
    private final int queuePosition;
    private final Write value;

    private final BoundedWindow window;

    WriteElement(int queuePosition, Write value, BoundedWindow window) {
      this.value = value;
      this.queuePosition = queuePosition;
      this.window = window;
    }

    public int getQueuePosition() {
      return queuePosition;
    }

    @Override
    public Write getValue() {
      return value;
    }

    @Override
    public long getSerializedSize() {
      return value.getSerializedSize();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof WriteElement)) {
        return false;
      }
      WriteElement that = (WriteElement) o;
      return queuePosition == that.queuePosition
          && value.equals(that.value)
          && window.equals(that.window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queuePosition, value, window);
    }

    @Override
    public String toString() {
      return "WriteElement{"
          + "queuePosition="
          + queuePosition
          + ", value="
          + value
          + ", window="
          + window
          + '}';
    }
  }
}
