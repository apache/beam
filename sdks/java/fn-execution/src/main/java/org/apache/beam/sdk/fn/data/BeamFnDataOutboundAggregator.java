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
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
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
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamFnDataOutboundAggregator {

  public static final String DATA_BUFFER_SIZE_LIMIT = "data_buffer_size_limit=";
  public static final int DEFAULT_BUFFER_LIMIT_BYTES = 1_000_000;
  public static final String DATA_BUFFER_TIME_LIMIT_MS = "data_buffer_time_limit_ms=";
  public static final long DEFAULT_BUFFER_LIMIT_TIME_MS = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataOutboundAggregator.class);
  private final int sizeLimit;
  private final long timeLimit;
  private final Supplier<String> processBundleRequestIdSupplier;
  private final Map<String, Receiver<?>> outputDataReceivers;
  private final Map<KV<String, String>, Receiver<?>> outputTimersReceivers;
  private final Map<LogicalEndpoint, Long> perEndpointElementCount;
  private final StreamObserver<Elements> outboundObserver;
  @VisibleForTesting ScheduledFuture<?> flushFuture;
  private final LongAdder totalByteCounter = new LongAdder();
  private final Object flushLock = new Object();

  public BeamFnDataOutboundAggregator(
      PipelineOptions options,
      Supplier<String> processBundleRequestIdSupplier,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    this.sizeLimit = getSizeLimit(options);
    this.timeLimit = getTimeLimit(options);
    this.outputDataReceivers = new HashMap<>();
    this.outputTimersReceivers = new HashMap<>();
    this.perEndpointElementCount = new HashMap<>();
    this.outboundObserver = outboundObserver;
    this.processBundleRequestIdSupplier = processBundleRequestIdSupplier;
  }

  /** Starts the flushing daemon thread if data_buffer_time_limit_ms is set. */
  public void startFlushThread() {
    if (timeLimit > 0 && this.flushFuture == null) {
      this.flushFuture =
          Executors.newSingleThreadScheduledExecutor(
                  new ThreadFactoryBuilder()
                      .setDaemon(true)
                      .setNameFormat("DataBufferOutboundFlusher-thread")
                      .build())
              .scheduleAtFixedRate(
                  this::periodicFlush, timeLimit, timeLimit, TimeUnit.MILLISECONDS);
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
    Receiver<T> receiver = new Receiver<>(coder, totalByteCounter);
    if (timeLimit > 0) {
      synchronized (flushLock) {
        outputDataReceivers.put(pTransformId, receiver);
        return data -> {
          checkFlushThreadException();
          synchronized (flushLock) {
            receiver.accept(data);
            if (totalByteCounter.longValue() > sizeLimit) {
              flush();
            }
          }
        };
      }
    }
    outputDataReceivers.put(pTransformId, receiver);
    return data -> {
      receiver.accept(data);
      if (totalByteCounter.longValue() > sizeLimit) {
        flush();
      }
    };
  }

  /**
   * Register the outbound timers logical endpoint, returns the FnDataReceiver for processing the
   * endpoint's outbound timers data.
   */
  public <T> FnDataReceiver<T> registerOutputTimersLocation(
      String pTransformId, String timerFamilyId, Coder<T> coder) {
    KV<String, String> timerKey = KV.of(pTransformId, timerFamilyId);
    if (outputTimersReceivers.containsKey(timerKey)) {
      throw new IllegalStateException(
          "Outbound timers endpoint already registered for " + timerKey);
    }
    Receiver<T> receiver = new Receiver<>(coder, totalByteCounter);
    if (timeLimit > 0) {
      synchronized (flushLock) {
        outputTimersReceivers.put(timerKey, receiver);
        return timers -> {
          checkFlushThreadException();
          synchronized (flushLock) {
            receiver.accept(timers);
            if (totalByteCounter.longValue() > sizeLimit) {
              flush();
            }
          }
        };
      }
    }
    outputTimersReceivers.put(timerKey, receiver);
    return timers -> {
      receiver.accept(timers);
      if (totalByteCounter.longValue() > sizeLimit) {
        flush();
      }
    };
  }

  public void flush() throws IOException {
    Elements.Builder elements = convertBufferForTransmission();
    if (elements.getDataCount() > 0 || elements.getTimersCount() > 0) {
      outboundObserver.onNext(elements.build());
    }
  }

  /**
   * Closes the streams for all registered outbound endpoints. Should be called at the end of each
   * bundle.
   */
  public void sendBufferedDataAndFinishOutboundStreams() {
    if (totalByteCounter.longValue() == 0
        && outputTimersReceivers.isEmpty()
        && outputDataReceivers.isEmpty()) {
      return;
    }
    Elements.Builder bufferedElements = convertBufferForTransmission();
    LOG.debug(
        "Closing streams for outbound endpoints {} and {}.",
        outputDataReceivers.keySet(),
        outputTimersReceivers.keySet());
    LOG.debug(
        "Sent outbound data size : {}, outbound timers size : {}.",
        outputDataReceivers.entrySet().stream()
            .map(kv -> new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue().localByteCount))
            .collect(Collectors.toSet()),
        outputTimersReceivers.entrySet().stream()
            .map(kv -> new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue().localByteCount))
            .collect(Collectors.toSet()));
    LOG.debug("Sent outbound element count : {}.", perEndpointElementCount);
    for (String pTransformID : outputDataReceivers.keySet()) {
      bufferedElements
          .addDataBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(pTransformID)
          .setIsLast(true);
    }
    for (KV<String, String> pTransformAndTimerId : outputTimersReceivers.keySet()) {
      bufferedElements
          .addTimersBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(pTransformAndTimerId.getKey())
          .setTimerFamilyId(pTransformAndTimerId.getValue())
          .setIsLast(true);
    }
    outboundObserver.onNext(bufferedElements.build());
    outputDataReceivers.values().forEach(Receiver::reset);
    outputTimersReceivers.values().forEach(Receiver::reset);
    perEndpointElementCount.clear();
  }

  private Elements.Builder convertBufferForTransmission() {
    Elements.Builder bufferedElements = Elements.newBuilder();
    for (Map.Entry<String, Receiver<?>> entry : outputDataReceivers.entrySet()) {
      ByteString bytes = entry.getValue().getOutput().toByteString();
      if (bytes.isEmpty()) {
        continue;
      }
      bufferedElements
          .addDataBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(entry.getKey())
          .setData(bytes);
      entry.getValue().reset();
      perEndpointElementCount.compute(
          LogicalEndpoint.data(processBundleRequestIdSupplier.get(), entry.getKey()),
          (e, count) -> count == null ? 1 : count + 1);
    }
    for (Map.Entry<KV<String, String>, Receiver<?>> entry : outputTimersReceivers.entrySet()) {
      ByteString bytes = entry.getValue().getOutput().toByteString();
      if (bytes.isEmpty()) {
        continue;
      }
      bufferedElements
          .addTimersBuilder()
          .setInstructionId(processBundleRequestIdSupplier.get())
          .setTransformId(entry.getKey().getKey())
          .setTimerFamilyId(entry.getKey().getValue())
          .setTimers(bytes);
      entry.getValue().reset();
      perEndpointElementCount.compute(
          LogicalEndpoint.timer(
              processBundleRequestIdSupplier.get(),
              entry.getKey().getKey(),
              entry.getKey().getValue()),
          (e, count) -> count == null ? 1 : count + 1);
    }
    totalByteCounter.reset();
    return bufferedElements;
  }

  private void periodicFlush() {
    try {
      synchronized (flushLock) {
        flush();
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

  private static class Receiver<T> implements FnDataReceiver<T> {
    private final ByteString.Output output;
    private final Coder<T> coder;
    private long localByteCount;
    private final LongAdder totalByteCounter;

    public Receiver(Coder<T> coder, LongAdder totalByteCounter) {
      this.output = ByteString.newOutput();
      this.coder = coder;
      this.totalByteCounter = totalByteCounter;
      this.localByteCount = 0L;
    }

    @Override
    public void accept(T input) throws Exception {
      int size = output.size();
      coder.encode(input, output);
      if (output.size() - size == 0) {
        output.write(0);
      }
      final long delta = (long) output.size() - size;
      totalByteCounter.add(delta);
      localByteCount += delta;
    }

    public ByteString.Output getOutput() {
      return output;
    }

    public void reset() {
      this.localByteCount = 0L;
      this.output.reset();
    }
  }
}
