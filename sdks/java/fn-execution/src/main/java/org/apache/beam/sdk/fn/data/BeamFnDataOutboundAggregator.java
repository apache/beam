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
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
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
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
// The calling thread that invokes sendBufferedDataAndFinishOutboundStreams synchronizes on
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
  private final Supplier<String> processBundleRequestIdSupplier;
  @VisibleForTesting final Map<String, Receiver<?>> outputDataReceivers;
  @VisibleForTesting final Map<TimerEndpoint, Receiver<?>> outputTimersReceivers;
  private final StreamObserver<Elements> outboundObserver;
  @Nullable @VisibleForTesting ScheduledFuture<?> flushFuture;
  private long bytesWrittenSinceFlush;
  private final Object flushLock;
  private final boolean collectElementsIfNoFlushes;
  private boolean hasFlushedForBundle;

  public BeamFnDataOutboundAggregator(
      PipelineOptions options,
      Supplier<String> processBundleRequestIdSupplier,
      StreamObserver<Elements> outboundObserver,
      boolean collectElementsIfNoFlushes) {
    this.sizeLimit = getSizeLimit(options);
    this.timeLimit = getTimeLimit(options);
    this.collectElementsIfNoFlushes = collectElementsIfNoFlushes;
    this.outputDataReceivers = new HashMap<>();
    this.outputTimersReceivers = new HashMap<>();
    this.outboundObserver = outboundObserver;
    this.processBundleRequestIdSupplier = processBundleRequestIdSupplier;
    this.bytesWrittenSinceFlush = 0L;
    this.flushLock = new Object();
    this.hasFlushedForBundle = false;
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
      outboundObserver.onNext(elements.build());
    }
    hasFlushedForBundle = true;
  }

  /**
   * Closes the streams for all registered outbound endpoints. Should be called at the end of each
   * bundle. Returns the buffered Elements if the BeamFnDataOutboundAggregator started with
   * collectElementsIfNoFlushes=true, and there was no previous flush in this bundle, otherwise
   * returns null.
   */
  public Elements sendOrCollectBufferedDataAndFinishOutboundStreams() {
    if (outputTimersReceivers.isEmpty() && outputDataReceivers.isEmpty()) {
      return null;
    }
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
        processBundleRequestIdSupplier.get(),
        outputDataReceivers,
        outputTimersReceivers);
    for (Map.Entry<String, Receiver<?>> entry : outputDataReceivers.entrySet()) {
      String pTransformId = entry.getKey();
      bufferedElements
          .addDataBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(pTransformId)
          .setIsLast(true);
      entry.getValue().resetStats();
    }
    for (Map.Entry<TimerEndpoint, Receiver<?>> entry : outputTimersReceivers.entrySet()) {
      TimerEndpoint timerKey = entry.getKey();
      bufferedElements
          .addTimersBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(timerKey.pTransformId)
          .setTimerFamilyId(timerKey.timerFamilyId)
          .setIsLast(true);
      entry.getValue().resetStats();
    }
    if (collectElementsIfNoFlushes && !hasFlushedForBundle) {
      return bufferedElements.build();
    }
    outboundObserver.onNext(bufferedElements.build());
    // This is now at the end of a bundle, so we reset hasFlushedForBundle to prepare for new
    // bundles.
    hasFlushedForBundle = false;
    return null;
  }

  // Send the elements to the StreamObserver associated with this aggregator.
  public void sendElements(Elements elements) {
    outboundObserver.onNext(elements);
  }

  public void discard() {
    if (flushFuture != null) {
      flushFuture.cancel(true);
    }
  }

  private Elements.Builder convertBufferForTransmission() {
    Elements.Builder bufferedElements = Elements.newBuilder();
    for (Map.Entry<String, Receiver<?>> entry : outputDataReceivers.entrySet()) {
      if (entry.getValue().bufferedSize() == 0) {
        continue;
      }
      ByteString bytes = entry.getValue().toByteStringAndResetBuffer();
      bufferedElements
          .addDataBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(entry.getKey())
          .setData(bytes);
    }
    for (Map.Entry<TimerEndpoint, Receiver<?>> entry : outputTimersReceivers.entrySet()) {
      if (entry.getValue().bufferedSize() == 0) {
        continue;
      }
      ByteString bytes = entry.getValue().toByteStringAndResetBuffer();
      bufferedElements
          .addTimersBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(entry.getKey().pTransformId)
          .setTimerFamilyId(entry.getKey().timerFamilyId)
          .setTimers(bytes);
    }
    bytesWrittenSinceFlush = 0L;
    return bufferedElements;
  }

  void flush() {
    try {
      synchronized (flushLock) {
        flushInternal();
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /** Check if the flush thread failed with an exception. */
  private void checkFlushThreadException() throws IOException {
    if (timeLimit > 0 && flushFuture.isDone()) {
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
      if (output.size() - size == 0) {
        output.write(0);
      }
      final long delta = (long) output.size() - size;
      bytesWrittenSinceFlush += delta;
      perBundleByteCount += delta;
      perBundleElementCount += 1;
      if (bytesWrittenSinceFlush > sizeLimit) {
        flushInternal();
      }
    }

    public long getByteCount() {
      return perBundleByteCount;
    }

    public long getElementCount() {
      return perBundleElementCount;
    }

    public int bufferedSize() {
      return output.size();
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
    public boolean equals(Object o) {
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
