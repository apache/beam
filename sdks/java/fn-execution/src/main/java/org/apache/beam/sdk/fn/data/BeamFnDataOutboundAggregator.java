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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
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
  private final Map<LogicalEndpoint, Coder<?>> outputLocations;
  private final Map<LogicalEndpoint, Long> perEndpointByteCount;
  private final Map<LogicalEndpoint, Long> perEndpointElementCount;
  private final StreamObserver<Elements> outboundObserver;
  private final Map<LogicalEndpoint, ByteString.Output> buffer;
  @VisibleForTesting ScheduledFuture<?> flushFuture;
  private final AtomicLong byteCounter = new AtomicLong(0L);

  public BeamFnDataOutboundAggregator(
      PipelineOptions options,
      Supplier<String> processBundleRequestIdSupplier,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    this.sizeLimit = getSizeLimit(options);
    this.timeLimit = getTimeLimit(options);
    this.outputLocations = new HashMap<>();
    this.perEndpointByteCount = new HashMap<>();
    this.perEndpointElementCount = new HashMap<>();
    this.outboundObserver = outboundObserver;
    this.buffer = new ConcurrentHashMap<>();
    if (timeLimit > 0) {
      this.flushFuture =
          Executors.newSingleThreadScheduledExecutor(
                  new ThreadFactoryBuilder()
                      .setDaemon(true)
                      .setNameFormat("DataBufferOutboundFlusher-thread")
                      .build())
              .scheduleAtFixedRate(
                  this::periodicFlush, timeLimit, timeLimit, TimeUnit.MILLISECONDS);
    } else {
      this.flushFuture = null;
    }
    this.processBundleRequestIdSupplier = processBundleRequestIdSupplier;
  }

  /**
   * Register the outbound data logical endpoint for the transform alongside it's output value
   * coder. All registered endpoints will eventually be removed when {@link
   * #sendBufferedDataAndFinishOutboundStreams()} is called at the end of a bundle.
   */
  public <T> void registerOutputDataLocation(String pTransformId, Coder<T> coder) {
    registerOutputLocation(
        LogicalEndpoint.data(processBundleRequestIdSupplier.get(), pTransformId), coder);
  }

  public <T> void registerOutputTimersLocation(
      String pTransformId, String timerFamilyId, Coder<T> coder) {
    registerOutputLocation(
        LogicalEndpoint.timer(processBundleRequestIdSupplier.get(), pTransformId, timerFamilyId),
        coder);
  }

  public <T> void registerOutputLocation(LogicalEndpoint endpoint, Coder<T> coder) {
    LOG.debug("Registering endpoint: {}", endpoint);
    outputLocations.put(endpoint, coder);
  }

  public synchronized void flush() throws IOException {
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
    LOG.debug("Closing streams for outbound endpoints {}", outputLocations);
    LOG.debug("Sent outbound data size : {}", perEndpointByteCount);
    LOG.debug("Sent outbound element count : {}", perEndpointElementCount);
    if (byteCounter.get() == 0 && outputLocations.isEmpty()) {
      return;
    }
    Elements.Builder bufferedElements = convertBufferForTransmission();
    for (LogicalEndpoint outputLocation : outputLocations.keySet()) {
      if (outputLocation.isTimer()) {
        bufferedElements
            .addTimersBuilder()
            .setInstructionId(outputLocation.getInstructionId())
            .setTransformId(outputLocation.getTransformId())
            .setTimerFamilyId(outputLocation.getTimerFamilyId())
            .setIsLast(true);
      } else {
        bufferedElements
            .addDataBuilder()
            .setInstructionId(outputLocation.getInstructionId())
            .setTransformId(outputLocation.getTransformId())
            .setIsLast(true);
      }
    }
    outboundObserver.onNext(bufferedElements.build());
    outputLocations.clear();
    perEndpointByteCount.clear();
    perEndpointElementCount.clear();
  }

  public <T> void acceptData(String pTransformId, T data) throws Exception {
    accept(LogicalEndpoint.data(processBundleRequestIdSupplier.get(), pTransformId), data);
  }

  public <T> void acceptTimers(String pTransformId, String timerFamilyId, T timers)
      throws Exception {
    accept(
        LogicalEndpoint.timer(processBundleRequestIdSupplier.get(), pTransformId, timerFamilyId),
        timers);
  }

  public <T> void accept(LogicalEndpoint endpoint, T data) throws Exception {
    if (timeLimit > 0) {
      checkFlushThreadException();
    }
    buffer.compute(
        endpoint,
        (e, output) -> {
          if (output == null) {
            output = ByteString.newOutput();
          }
          int size = output.size();
          try {
            ((Coder<T>) outputLocations.get(e)).encode(data, output);
          } catch (IOException ex) {
            throw new RuntimeException("Failed to encode data.");
          }
          if (output.size() - size == 0) {
            output.write(0);
          }
          final long delta = (long) output.size() - size;
          byteCounter.getAndAdd(delta);
          perEndpointByteCount.compute(
              e, (e1, byteCount) -> byteCount == null ? delta : delta + byteCount);
          return output;
        });

    if (byteCounter.get() >= sizeLimit) {
      flush();
    }
  }

  private Elements.Builder convertBufferForTransmission() {
    Elements.Builder bufferedElements = Elements.newBuilder();
    for (LogicalEndpoint endpoint : buffer.keySet()) {
      ByteString.Output output = buffer.remove(endpoint);
      if (output == null) {
        continue;
      }
      byteCounter.getAndAdd(-1 * output.size());
      if (endpoint.isTimer()) {
        bufferedElements
            .addTimersBuilder()
            .setInstructionId(endpoint.getInstructionId())
            .setTransformId(endpoint.getTransformId())
            .setTimerFamilyId(endpoint.getTimerFamilyId())
            .setTimers(output.toByteString());
      } else {
        bufferedElements
            .addDataBuilder()
            .setInstructionId(endpoint.getInstructionId())
            .setTransformId(endpoint.getTransformId())
            .setData(output.toByteString());
      }
      perEndpointElementCount.compute(endpoint, (e, count) -> count == null ? 1 : count + 1);
      output.reset();
    }
    return bufferedElements;
  }

  private void periodicFlush() {
    try {
      flush();
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
}
