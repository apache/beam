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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.CancellableQueue;

/**
 * Decodes {@link BeamFnApi.Elements} partitioning them using the provided {@link DataEndpoint}s and
 * {@link TimerEndpoint}s.
 *
 * <p>Note that this receiver uses a queue to buffer and pass elements from one thread to be
 * processed by the thread which invokes {@link #awaitCompletion}.
 *
 * <p>Closing the receiver will unblock any upstream producer and downstream consumer exceptionally.
 */
public class BeamFnDataInboundObserver implements CloseableFnDataReceiver<BeamFnApi.Elements> {

  /**
   * Creates a receiver that is able to consume elements multiplexing on to the provided set of
   * endpoints.
   */
  public static BeamFnDataInboundObserver forConsumers(
      List<DataEndpoint<?>> dataEndpoints, List<TimerEndpoint<?>> timerEndpoints) {
    return new BeamFnDataInboundObserver(dataEndpoints, timerEndpoints);
  }

  /** Holds the status of whether the endpoint has been completed or not. */
  private static class EndpointStatus<T> {
    final T endpoint;
    boolean isDone;

    EndpointStatus(T endpoint) {
      this.endpoint = endpoint;
    }
  }

  private final Map<String, EndpointStatus<DataEndpoint<?>>> transformIdToDataEndpoint;
  private final Map<String, Map<String, EndpointStatus<TimerEndpoint<?>>>>
      transformIdToTimerFamilyIdToTimerEndpoint;
  private final CancellableQueue<BeamFnApi.Elements> queue;
  // We use a custom exception for closing to avoid the expense of stack trace generation.
  protected static class CloseException extends Exception {
    private CloseException() {
      super(
          "Inbound observer closed.",
          null,
          /*enableSuppression=*/ false,
          /*writableStackTrace=*/ false);
    }

    public static final CloseException INSTANCE = new CloseException();
  }

  private final int totalNumEndpoints;
  private int numEndpointsThatAreIncomplete;

  private AtomicBoolean consumingReceivedData;

  private BeamFnDataInboundObserver(
      List<DataEndpoint<?>> dataEndpoints, List<TimerEndpoint<?>> timerEndpoints) {
    this.transformIdToDataEndpoint = new HashMap<>();
    for (DataEndpoint<?> endpoint : dataEndpoints) {
      transformIdToDataEndpoint.put(endpoint.getTransformId(), new EndpointStatus<>(endpoint));
    }
    this.transformIdToTimerFamilyIdToTimerEndpoint = new HashMap<>();
    for (TimerEndpoint<?> endpoint : timerEndpoints) {
      transformIdToTimerFamilyIdToTimerEndpoint
          .computeIfAbsent(endpoint.getTransformId(), unused -> new HashMap<>())
          .put(endpoint.getTimerFamilyId(), new EndpointStatus<>(endpoint));
    }
    this.queue = new CancellableQueue<>(100);
    this.totalNumEndpoints = dataEndpoints.size() + timerEndpoints.size();
    this.numEndpointsThatAreIncomplete = totalNumEndpoints;
    this.consumingReceivedData = new AtomicBoolean(false);
  }

  @Override
  public void accept(BeamFnApi.Elements elements) throws Exception {
    queue.put(elements);
  }

  @Override
  public void flush() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws Exception {
    queue.cancel(CloseException.INSTANCE);
  }

  public boolean isConsumingReceivedData() {
    return consumingReceivedData.get();
  }

  /**
   * Uses the callers thread to process all elements received until we receive the end of the stream
   * from the upstream producer for all endpoints specified.
   *
   * <p>Erroneous elements passed from the producer will be visible to the caller of this method.
   */
  public void awaitCompletion() throws Exception {
    try {
      while (true) {
        // The SDK indicates it has consumed all the received data before it attempts to take
        // more elements off the queue.
        consumingReceivedData.set(false);
        BeamFnApi.Elements elements = queue.take();
        // The SDK is no longer blocked on receiving more data from the Runner.
        consumingReceivedData.set(true);
        if (multiplexElements(elements)) {
          return;
        }
      }
    } catch (Exception e) {
      queue.cancel(e);
      throw e;
    } finally {
      consumingReceivedData.set(false);
      close();
    }
  }

  /**
   * Dispatches the data and timers from the elements to corresponding receivers. Returns true if
   * all the endpoints are done after elements dispatching.
   */
  public boolean multiplexElements(Elements elements) throws Exception {
    for (BeamFnApi.Elements.Data data : elements.getDataList()) {
      EndpointStatus<DataEndpoint<?>> endpoint =
          transformIdToDataEndpoint.get(data.getTransformId());
      if (endpoint == null) {
        throw new IllegalStateException(
            String.format(
                "Unable to find inbound data receiver for instruction %s and transform %s.",
                data.getInstructionId(), data.getTransformId()));
      } else if (endpoint.isDone) {
        throw new IllegalStateException(
            String.format(
                "Received data after inbound data receiver is done for instruction %s and transform %s.",
                data.getInstructionId(), data.getTransformId()));
      }
      InputStream inputStream = data.getData().newInput();
      Coder<Object> coder = (Coder<Object>) endpoint.endpoint.getCoder();
      FnDataReceiver<Object> receiver = (FnDataReceiver<Object>) endpoint.endpoint.getReceiver();
      while (inputStream.available() > 0) {
        receiver.accept(coder.decode(inputStream));
      }
      if (data.getIsLast()) {
        endpoint.isDone = true;
        numEndpointsThatAreIncomplete -= 1;
      }
    }

    for (BeamFnApi.Elements.Timers timers : elements.getTimersList()) {
      Map<String, EndpointStatus<TimerEndpoint<?>>> timerFamilyIdToEndpoints =
          transformIdToTimerFamilyIdToTimerEndpoint.get(timers.getTransformId());
      if (timerFamilyIdToEndpoints == null) {
        throw new IllegalStateException(
            String.format(
                "Unable to find inbound timer receiver for instruction %s, transform %s, and timer family %s.",
                timers.getInstructionId(), timers.getTransformId(), timers.getTimerFamilyId()));
      }
      EndpointStatus<TimerEndpoint<?>> endpoint =
          timerFamilyIdToEndpoints.get(timers.getTimerFamilyId());
      if (endpoint == null) {
        throw new IllegalStateException(
            String.format(
                "Unable to find inbound timer receiver for instruction %s, transform %s, and timer family %s.",
                timers.getInstructionId(), timers.getTransformId(), timers.getTimerFamilyId()));
      } else if (endpoint.isDone) {
        throw new IllegalStateException(
            String.format(
                "Received timer after inbound timer receiver is done for instruction %s, transform %s, and timer family %s.",
                timers.getInstructionId(), timers.getTransformId(), timers.getTimerFamilyId()));
      }
      InputStream inputStream = timers.getTimers().newInput();
      Coder<Object> coder = (Coder<Object>) endpoint.endpoint.getCoder();
      FnDataReceiver<Object> receiver = (FnDataReceiver<Object>) endpoint.endpoint.getReceiver();
      while (inputStream.available() > 0) {
        receiver.accept(coder.decode(inputStream));
      }
      if (timers.getIsLast()) {
        endpoint.isDone = true;
        numEndpointsThatAreIncomplete -= 1;
      }
    }
    return numEndpointsThatAreIncomplete == 0;
  }

  /** Enables this receiver to be used again for another bundle. */
  public void reset() {
    numEndpointsThatAreIncomplete = totalNumEndpoints;
    for (EndpointStatus<?> value : transformIdToDataEndpoint.values()) {
      value.isDone = false;
    }
    for (Map<String, EndpointStatus<TimerEndpoint<?>>> value :
        transformIdToTimerFamilyIdToTimerEndpoint.values()) {
      for (EndpointStatus<?> status : value.values()) {
        status.isDone = false;
      }
    }
    queue.reset();
  }

  /**
   * Get all unfinished data and timers endpoints represented as [transform_id]:data and
   * [transform_id]:timers:[timer_family_id].
   */
  public List<String> getUnfinishedEndpoints() {
    List<String> unfinishedEndpoints = new ArrayList<>();
    for (Entry<String, EndpointStatus<DataEndpoint<?>>> endpointStatus :
        transformIdToDataEndpoint.entrySet()) {
      if (!endpointStatus.getValue().isDone) {
        unfinishedEndpoints.add(String.format("%s:data", endpointStatus.getKey()));
      }
    }
    for (Entry<String, Map<String, EndpointStatus<TimerEndpoint<?>>>> entry :
        transformIdToTimerFamilyIdToTimerEndpoint.entrySet()) {
      for (Entry<String, EndpointStatus<TimerEndpoint<?>>> timerFamilyStatus :
          entry.getValue().entrySet()) {
        if (!timerFamilyStatus.getValue().isDone) {
          unfinishedEndpoints.add(
              String.format("%s:timers:%s", entry.getKey(), timerFamilyStatus.getKey()));
        }
      }
    }
    return unfinishedEndpoints;
  }
}
