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
package org.apache.beam.sdk.fn.data;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An outbound data buffering aggregator with size-based buffer and time-based buffer if
 * corresponding options are set.
 *
 * <p>The default size-based buffer threshold can be overridden by specifying the experiment {@code
 * data_buffer_size_limit=<bytes>}
 *
 * <p>The default time-based buffer threshold can be overridden by specifying the experiment {@code
 * data_buffer_time_limit_ms=<milliseconds>}
 */
// The calling thread that invokes sendOrCollectBufferedDataAndFinishOutboundStreams synchronizes on
// flushLock effectively making the periodic flushing no longer read or mutate hasFlushedForBundle
// and allowing the calling thread to read and mutate hasFlushedForBundle safely without needing to
// create another memory barrier. Also note that flush is always invoked when synchronizing on
// flushLock when there is a periodic flushing thread.
@NotThreadSafe
public class BeamFnDataOutboundAggregator {

  public static final String DATA_BUFFER_SIZE_LIMIT = "data_buffer_size_limit=";
  public static final int DEFAULT_BUFFER_LIMIT_BYTES = 1_000_000;
  public static final String DATA_BUFFER_TIME_LIMIT_MS = "data_buffer_time_limit_ms=";
  public static final long DEFAULT_BUFFER_LIMIT_TIME_MS = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataOutboundAggregator.class);
  private final int sizeLimit;
  private final long timeLimit;
  // The instructionId is set between prepareForInstruction and finishInstruction/discard.
  private @Nullable String instructionId = null;
  @VisibleForTesting final Map<String, Receiver<?>> outputDataReceivers = new HashMap<>();
  @VisibleForTesting final Map<TimerEndpoint, Receiver<?>> outputTimersReceivers = new HashMap<>();
  @Nullable private StreamObserver<Elements> outboundObserver;
  @Nullable @VisibleForTesting ScheduledFuture<?> flushFuture;
  private long bytesWrittenSinceFlush = 0;
  private final Object flushLock = new Object();
  private final boolean collectElementsIfNoFlushes;
  private boolean hasFlushedForBundle = false;

  public BeamFnDataOutboundAggregator(PipelineOptions options, boolean collectElementsIfNoFlushes) {
    this.sizeLimit = getSizeLimit(options);
    this.timeLimit = getTimeLimit(options);
    this.collectElementsIfNoFlushes = collectElementsIfNoFlushes;
  }

  public void prepareForInstruction(
      String instructionId, StreamObserver<Elements> outboundObserver) {
    if (timeLimit > 0) {
      synchronized (flushLock) {
        checkState(this.instructionId == null && this.outboundObserver == null);
        this.instructionId = instructionId;
        this.outboundObserver = outboundObserver;
      }
    } else {
      checkState(this.instructionId == null && this.outboundObserver == null);
      this.instructionId = instructionId;
      this.outboundObserver = outboundObserver;
    }
  }

  public void finishInstruction() {
    if (flushFuture != null) {
      synchronized (flushLock) {
        checkState(
            this.instructionId != null && this.outboundObserver != null,
            "instruction was not started or previously completed");
        checkState(bytesWrittenSinceFlush == 0, "bytes were not flushed for instruction");
        this.instructionId = null;
        this.outboundObserver = null;
      }
    } else {
      checkState(this.instructionId != null && this.outboundObserver != null);
      checkState(bytesWrittenSinceFlush == 0, "bytes were not flushed for instruction");
      this.instructionId = null;
      this.outboundObserver = null;
    }
  }

  /** Starts the flushing daemon thread if data_buffer_time_limit_ms is set. */
  public void start() {
    if (timeLimit > 0 && this.flushFuture == null) {
      this.flushFuture =
          Executors.newSingleThreadScheduledExecutor(
                  new ThreadFactoryBuilder()
                      .setDaemon(true)
                      .setNameFormat("DataBufferOutboundFlusher-thread")
                      .build())
              .scheduleAtFixedRate(this::flush, timeLimit, timeLimit, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Register the outbound data logical endpoint, returns the FnDataReceiver for processing the
   * endpoint's outbound data.
   */
  public <T> FnDataReceiver<T> registerOutputDataLocation(String pTransformId, Coder<T> coder) {
    if (outputDataReceivers.containsKey(pTransformId)) {
      throw new IllegalStateException(
          "Outbound data endpoint already registered for " + pTransformId);
    }
    Receiver<T> receiver = new Receiver<>(coder);
    if (timeLimit > 0) {
      outputDataReceivers.put(pTransformId, receiver);
      return data -> {
        checkFlushThreadException();
        synchronized (flushLock) {
          receiver.accept(data);
        }
      };
    }
    outputDataReceivers.put(pTransformId, receiver);
    return receiver;
  }

  /**
   * Register the outbound timers logical endpoint, returns the FnDataReceiver for processing the
   * endpoint's outbound timers data.
   */
  public <T> FnDataReceiver<T> registerOutputTimersLocation(
      String pTransformId, String timerFamilyId, Coder<T> coder) {
    TimerEndpoint timerKey = new TimerEndpoint(pTransformId, timerFamilyId);
    if (outputTimersReceivers.containsKey(timerKey)) {
      throw new IllegalStateException(
          "Outbound timers endpoint already registered for " + timerKey);
    }
    Receiver<T> receiver = new Receiver<>(coder);
    if (timeLimit > 0) {
      outputTimersReceivers.put(timerKey, receiver);
      return timers -> {
        checkFlushThreadException();
        synchronized (flushLock) {
          receiver.accept(timers);
        }
      };
    }
    outputTimersReceivers.put(timerKey, receiver);
    return receiver;
  }

  private void flushInternal() {
    if (bytesWrittenSinceFlush == 0) {
      return;
    }
    Elements.Builder elements = convertBufferForTransmission();
    if (elements.getDataCount() > 0 || elements.getTimersCount() > 0) {
      checkNotNull(outboundObserver).onNext(elements.build());
    }
    hasFlushedForBundle = true;
  }

  /**
   * Closes the streams for all registered outbound endpoints. Should be called at the end of each
   * bundle. Returns the buffered Elements if the BeamFnDataOutboundAggregator started with
   * collectElementsIfNoFlushes=true, and there was no previous flush in this bundle, otherwise
   * returns null.
   */
  @Nullable
  public Elements sendOrCollectBufferedDataAndFinishOutboundStreams() {
    if (outputTimersReceivers.isEmpty() && outputDataReceivers.isEmpty()) {
      return null;
    }
    String instructionId =
        checkNotNull(
            this.instructionId,
            "This method should only be called between prepareForInstruction and finishInstruction");
    Elements.Builder bufferedElements;
    if (timeLimit > 0) {
      synchronized (flushLock) {
        bufferedElements = convertBufferForTransmission();
      }
    } else {
      bufferedElements = convertBufferForTransmission();
    }
    LOG.debug(
        "Closing streams for instruction {} and outbound data {} and timers {}.",
        instructionId,
        outputDataReceivers,
        outputTimersReceivers);
    for (Map.Entry<String, Receiver<?>> entry : outputDataReceivers.entrySet()) {
      String pTransformId = entry.getKey();
      bufferedElements
          .addDataBuilder()
          .setInstructionId(instructionId)
          .setTransformId(pTransformId)
          .setIsLast(true);
      entry.getValue().resetStats();
    }
    for (Map.Entry<TimerEndpoint, Receiver<?>> entry : outputTimersReceivers.entrySet()) {
      TimerEndpoint timerKey = entry.getKey();
      bufferedElements
          .addTimersBuilder()
          .setInstructionId(instructionId)
          .setTransformId(timerKey.pTransformId)
          .setTimerFamilyId(timerKey.timerFamilyId)
          .setIsLast(true);
      entry.getValue().resetStats();
    }
    // This is the end of the bundle so we reset state to prepare for future bundles.
    if (collectElementsIfNoFlushes && !hasFlushedForBundle) {
      return bufferedElements.build();
    }
    checkNotNull(outboundObserver).onNext(bufferedElements.build());
    hasFlushedForBundle = false;
    return null;
  }

  // Send the elements to the StreamObserver associated with this aggregator.
  public void sendElements(Elements elements) {
    if (timeLimit > 0) {
      synchronized (flushLock) {
        checkNotNull(outboundObserver).onNext(elements);
      }
    } else {
      checkNotNull(outboundObserver).onNext(elements);
    }
  }

  // Prepares for discarding the aggregator without preserving its output or
  // preparing it for reuse.
  public void discard() {
    if (timeLimit > 0) {
      // Short-circuit the possibly concurrently running flush.
      synchronized (flushLock) {
        bytesWrittenSinceFlush = 0L;
        finishInstruction();
      }
      if (flushFuture != null) {
        flushFuture.cancel(false);
      }
    } else {
      bytesWrittenSinceFlush = 0L;
      finishInstruction();
    }
  }

  private Elements.Builder convertBufferForTransmission() {
    Elements.Builder bufferedElements = Elements.newBuilder();
    if (bytesWrittenSinceFlush == 0) {
      return bufferedElements;
    }
    bytesWrittenSinceFlush = 0L;
    String instructionId =
        checkNotNull(
            this.instructionId,
            "This method should only be called between prepareForInstruction and finishInstruction");
    for (Map.Entry<String, Receiver<?>> entry : outputDataReceivers.entrySet()) {
      if (!entry.getValue().hasBufferedOutput()) {
        continue;
      }
      ByteString bytes = entry.getValue().toByteStringAndResetBuffer();
      bufferedElements
          .addDataBuilder()
          .setInstructionId(instructionId)
          .setTransformId(entry.getKey())
          .setData(bytes);
    }
    for (Map.Entry<TimerEndpoint, Receiver<?>> entry : outputTimersReceivers.entrySet()) {
      if (!entry.getValue().hasBufferedOutput()) {
        continue;
      }
      ByteString bytes = entry.getValue().toByteStringAndResetBuffer();
      bufferedElements
          .addTimersBuilder()
          .setInstructionId(instructionId)
          .setTransformId(entry.getKey().pTransformId)
          .setTimerFamilyId(entry.getKey().timerFamilyId)
          .setTimers(bytes);
    }
    return bufferedElements;
  }

  void flush() {
    try {
      synchronized (flushLock) {
        flushInternal();
      }
    } catch (OutOfMemoryError oom) {
      throw oom;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /** Check if the flush thread failed with an exception. */
  private void checkFlushThreadException() throws IOException {
    if (flushFuture != null && flushFuture.isDone()) {
      try {
        flushFuture.get();
        throw new IOException("Periodic flushing thread finished unexpectedly.");
      } catch (ExecutionException ee) {
        unwrapExecutionException(ee);
      } catch (CancellationException ce) {
        throw new IOException(ce);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException(ie);
      }
    }
  }

  private void unwrapExecutionException(ExecutionException ee) throws IOException {
    // the cause is always RuntimeException
    RuntimeException re = (RuntimeException) ee.getCause();
    if (re.getCause() instanceof IOException) {
      throw (IOException) re.getCause();
    } else {
      throw new IOException(re.getCause());
    }
  }

  private static int getSizeLimit(PipelineOptions options) {
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    for (String experiment : experiments == null ? Collections.<String>emptyList() : experiments) {
      if (experiment.startsWith(DATA_BUFFER_SIZE_LIMIT)) {
        return Integer.parseInt(experiment.substring(DATA_BUFFER_SIZE_LIMIT.length()));
      }
    }
    return DEFAULT_BUFFER_LIMIT_BYTES;
  }

  private static long getTimeLimit(PipelineOptions options) {
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    for (String experiment : experiments == null ? Collections.<String>emptyList() : experiments) {
      if (experiment.startsWith(DATA_BUFFER_TIME_LIMIT_MS)) {
        return Long.parseLong(experiment.substring(DATA_BUFFER_TIME_LIMIT_MS.length()));
      }
    }
    return DEFAULT_BUFFER_LIMIT_TIME_MS;
  }

  @VisibleForTesting
  class Receiver<T> implements FnDataReceiver<T> {
    private final ByteStringOutputStream output;
    private final Coder<T> coder;
    private long perBundleByteCount;
    private long perBundleElementCount;

    public Receiver(Coder<T> coder) {
      this.output = new ByteStringOutputStream();
      this.coder = coder;
      this.perBundleByteCount = 0L;
      this.perBundleElementCount = 0L;
    }

    @Override
    public void accept(T input) throws Exception {
      int size = output.size();
      coder.encode(input, output);
      long delta = (long) output.size() - size;
      if (delta == 0) {
        output.write(0);
        delta = 1;
      }
      bytesWrittenSinceFlush += delta;
      perBundleByteCount += delta;
      perBundleElementCount += 1;
      if (bytesWrittenSinceFlush > sizeLimit) {
        flushInternal();
      }
    }

    @VisibleForTesting
    public long getByteCount() {
      return perBundleByteCount;
    }

    @VisibleForTesting
    public long getElementCount() {
      return perBundleElementCount;
    }

    public boolean hasBufferedOutput() {
      return !output.isEmpty();
    }

    public ByteString toByteStringAndResetBuffer() {
      return this.output.toByteStringAndReset();
    }

    public void resetStats() {
      this.perBundleElementCount = 0L;
      this.perBundleByteCount = 0L;
    }

    @Override
    public String toString() {
      return String.format(
          "Byte size: %s, Element count: %s", perBundleByteCount, perBundleElementCount);
    }
  }

  private static class TimerEndpoint {

    private final String pTransformId;
    private final String timerFamilyId;

    public TimerEndpoint(String pTransformId, String timerFamilyId) {
      this.pTransformId = pTransformId;
      this.timerFamilyId = timerFamilyId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimerEndpoint)) {
        return false;
      }
      TimerEndpoint that = (TimerEndpoint) o;
      return pTransformId.equals(that.pTransformId) && timerFamilyId.equals(that.timerFamilyId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pTransformId, timerFamilyId);
    }

    @Override
    public String toString() {
      return "pTransformId: " + pTransformId + " timerFamilyId: " + timerFamilyId;
    }
  }
}
