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
package org.apache.beam.fn.harness.data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that queues elements so that they can be consumed and processed in the
 * thread which calls @{link #drainAndBlock}.
 */
public class QueueingBeamFnDataClient implements BeamFnDataClient {

  private static final int QUEUE_SIZE = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(QueueingBeamFnDataClient.class);

  private final BeamFnDataClient mainClient;
  private final LinkedBlockingQueue<ConsumerAndData> queue;
  private final ConcurrentHashMap<InboundDataClient, Object> inboundDataClients;

  public QueueingBeamFnDataClient(BeamFnDataClient mainClient) {
    this.mainClient = mainClient;
    this.queue = new LinkedBlockingQueue<ConsumerAndData>(QUEUE_SIZE);
    this.inboundDataClients = new ConcurrentHashMap<>();
  }

  @Override
  public <T> InboundDataClient receive(
      ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      Coder<T> coder,
      FnDataReceiver<T> consumer) {
    LOG.debug(
        "Registering consumer for instruction {} and transform {}",
        inputLocation.getInstructionId(),
        inputLocation.getTransformId());

    QueueingFnDataReceiver<T> queueingConsumer = new QueueingFnDataReceiver<T>(consumer);
    InboundDataClient inboundDataClient =
        this.mainClient.receive(apiServiceDescriptor, inputLocation, coder, queueingConsumer);
    queueingConsumer.inboundDataClient = inboundDataClient;
    this.inboundDataClients.computeIfAbsent(
        inboundDataClient, (InboundDataClient idcToStore) -> idcToStore);
    return inboundDataClient;
  }

  // Returns true if all the InboundDataClients have finished or cancelled and no values
  // remain on the queue.
  private boolean allDone() {
    for (InboundDataClient inboundDataClient : inboundDataClients.keySet()) {
      if (!inboundDataClient.isDone()) {
        return false;
      }
    }
    if (!this.queue.isEmpty()) {
      return false;
    }
    return true;
  }

  /**
   * Drains the internal queue of this class, by waiting for all values to be passed to their
   * consumers. The thread which wishes to process() the elements should call this method, as this
   * will cause the consumers to invoke element processing. All receive() and send() calls must be
   * made prior to calling drainAndBlock, in order to properly terminate.
   *
   * <p>All {@link InboundDataClient}s will be failed if processing throws an exception.
   *
   * <p>This method is NOT thread safe. This should only be invoked by a single thread, and is
   * intended for use with a newly constructed QueueingBeamFnDataClient in {@link
   * ProcessBundleHandler#processBundle}.
   */
  public void drainAndBlock() throws Exception {
    while (true) {
      try {
        ConsumerAndData tuple = queue.poll(200, TimeUnit.MILLISECONDS);
        if (tuple != null) {
          // Forward to the consumers who cares about this data.
          tuple.consumer.accept(tuple.data);
        } else {
          // Note: We do not expect to ever hit this point without receiving all values
          // as (1) The InboundObserver will not be set to Done until the
          // QueuingFnDataReceiver.accept() call returns and will not be invoked again.
          // (2) The QueueingFnDataReceiver will not return until the value is received in
          // drainAndBlock, because of the use of the SynchronousQueue.
          if (allDone()) {
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Client failed to dequeue and process the value", e);
        for (InboundDataClient inboundDataClient : inboundDataClients.keySet()) {
          inboundDataClient.fail(e);
        }
        throw e;
      }
    }
  }

  @Override
  public <T> CloseableFnDataReceiver<T> send(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint outputLocation,
      Coder<T> coder) {
    LOG.debug(
        "Creating output consumer for instruction {} and transform {}",
        outputLocation.getInstructionId(),
        outputLocation.getTransformId());
    return this.mainClient.send(apiServiceDescriptor, outputLocation, coder);
  }

  /**
   * The QueueingFnDataReceiver is a a FnDataReceiver used by the QueueingBeamFnDataClient.
   *
   * <p>All {@link #accept accept()ed} values will be put onto a synchronous queue which will cause
   * the calling thread to block until {@link QueueingBeamFnDataClient#drainAndBlock} is called.
   * {@link QueueingBeamFnDataClient#drainAndBlock} is responsible for processing values from the
   * queue.
   */
  public class QueueingFnDataReceiver<T> implements FnDataReceiver<T> {
    private final FnDataReceiver<T> consumer;
    public InboundDataClient inboundDataClient;

    public QueueingFnDataReceiver(FnDataReceiver<T> consumer) {
      this.consumer = consumer;
    }

    /**
     * This method is thread safe, we expect multiple threads to call this, passing in data when new
     * data arrives via the QueueingBeamFnDataClient's mainClient.
     */
    @Override
    public void accept(T value) throws Exception {
      try {
        ConsumerAndData offering = new ConsumerAndData(this.consumer, value);
        while (!queue.offer(offering, 200, TimeUnit.MILLISECONDS)) {
          if (inboundDataClient.isDone()) {
            // If it was cancelled by the consuming side of the queue.
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to insert the value into the queue", e);
        inboundDataClient.fail(e);
        throw e;
      }
    }
  }

  static class ConsumerAndData<T> {
    public FnDataReceiver<T> consumer;
    public T data;

    public ConsumerAndData(FnDataReceiver<T> receiver, T data) {
      this.consumer = receiver;
      this.data = data;
    }
  }
}
