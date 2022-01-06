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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p36p0.io.grpc.stub.StreamObserver;
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
  private final Set<LogicalEndpoint> outputLocations;
  private final Set<LogicalEndpoint> unregisteredOutputLocations;
  private final StreamObserver<Elements> outboundObserver;
  private final Map<LogicalEndpoint, ByteString.Output> buffer;
  @VisibleForTesting ScheduledFuture<?> flushFuture;
  private long byteCounter;

  public BeamFnDataOutboundAggregator(
      PipelineOptions options, StreamObserver<BeamFnApi.Elements> outboundObserver) {
    this.sizeLimit = getSizeLimit(options);
    this.timeLimit = getTimeLimit(options);
    this.outputLocations = new HashSet<>();
    this.unregisteredOutputLocations = new HashSet<>();
    this.buffer = new ConcurrentHashMap<>();
    this.outboundObserver = outboundObserver;
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
  }

  public synchronized void registerOutputLocation(LogicalEndpoint endpoint) {
    LOG.debug("Registering endpoint: {}", endpoint);
    outputLocations.add(endpoint);
  }

  public synchronized void unregisterOutputLocation(LogicalEndpoint endpoint) {
    LOG.debug("UnRegistering endpoint: {}", endpoint);
    unregisteredOutputLocations.add(endpoint);
    if (unregisteredOutputLocations.equals(outputLocations)) {
      sendBufferedDataAndFinishOutbounds();
    }
  }

  public void flush() throws IOException {
    if (byteCounter > 0) {
      outboundObserver.onNext(convertBufferForTransmission().build());
    }
  }

  public void sendBufferedDataAndFinishOutbounds() {
    LOG.debug("Closing streams for outbound endpoints {}", outputLocations);
    if (byteCounter == 0 && outputLocations.isEmpty()) {
      return;
    }
    Elements.Builder bufferedElements = convertBufferForTransmission();
    for (LogicalEndpoint outputLocation : outputLocations) {
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

    // This bundle is finished we can clear outputLocations. New outputLocations should be
    // registered in startBundle phase for next bundle.
    outputLocations.clear();
    unregisteredOutputLocations.clear();
  }

  public void accept(LogicalEndpoint endpoint, ByteString.Output data) throws Exception {
    if (timeLimit > 0) {
      checkFlushThreadException();
    }
    data.writeTo(buffer.computeIfAbsent(endpoint, e -> ByteString.newOutput()));
    byteCounter += data.size();

    if (byteCounter >= sizeLimit) {
      flush();
    }
  }

  private Elements.Builder convertBufferForTransmission() {
    Elements.Builder bufferedElements = Elements.newBuilder();
    for (Map.Entry<LogicalEndpoint, ByteString.Output> bufferEntry : buffer.entrySet()) {
      LogicalEndpoint endpoint = bufferEntry.getKey();
      if (endpoint.isTimer()) {
        bufferedElements
            .addTimersBuilder()
            .setInstructionId(endpoint.getInstructionId())
            .setTransformId(endpoint.getTransformId())
            .setTimerFamilyId(endpoint.getTimerFamilyId())
            .setTimers(bufferEntry.getValue().toByteString());
      } else {
        bufferedElements
            .addDataBuilder()
            .setInstructionId(endpoint.getInstructionId())
            .setTransformId(endpoint.getTransformId())
            .setData(bufferEntry.getValue().toByteString());
      }
    }
    byteCounter = 0;
    buffer.clear();
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
