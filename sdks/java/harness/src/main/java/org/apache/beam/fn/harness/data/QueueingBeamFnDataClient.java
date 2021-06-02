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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that queues elements so that they can be consumed and processed in the
 * thread which calls @{link #drainAndBlock}.
 *
 * <p>This class is ready for use after creation or after a call to {@link #reset}. During usage it
 * is expected that possibly multiple threads will register clients with {@link #receive}. After all
 * clients have been registered a single thread should call {@link #drainAndBlock}.
 */
public class QueueingBeamFnDataClient implements BeamFnDataClient {
  private static class ClosableQueue {
    // Used to indicate that the queue is closed.
    private static final ConsumerAndData<Object> POISON =
        new ConsumerAndData<>(
            input -> {
              throw new RuntimeException("Unable to accept poison.");
            },
            new Object());

    private final LinkedBlockingQueue<ConsumerAndData<?>> queue;
    private final AtomicBoolean closed = new AtomicBoolean();

    ClosableQueue(int queueSize) {
      this.queue = new LinkedBlockingQueue<ConsumerAndData<?>>(queueSize);
    }

    // Closes the queue indicating that no additional elements will be added. May be called
    // at most once. Non-blocking.
    void close() {
      Preconditions.checkArgument(!closed.getAndSet(true));
      if (!queue.offer(POISON)) {
        // If this returns false, the queue was full. Since there were elements in
        // the queue, the poison is unnecessary since we check closed when taking from
        // the queue.
        LOG.debug("Queue was full, not adding poison");
      }
    }

    // See BlockingQueue.offer. Must not be called after close().
    boolean offer(ConsumerAndData<?> e, long l, TimeUnit t) throws InterruptedException {
      return queue.offer(e, l, t);
    }

    // Blocks until there is either an element available or the queue has been closed and is
    // empty in which case it returns null.
    @Nullable
    ConsumerAndData<?> take() throws InterruptedException {
      // We first poll without blocking to optimize for the case there is data.
      // If there is no data we end up blocking on take() and thus the extra
      // poll doesn't matter.
      @Nullable ConsumerAndData<?> result = queue.poll();
      if (result == null) {
        if (closed.get()) {
          // Poll again to ensure that there is nothing in the queue. Once we observe closed as true
          // we are guaranteed no additional elements other than the POISON will be added. However
          // we can't rely on the previous poll result as it could race with additional offers and
          // close.
          result = queue.poll();
        } else {
          // We are not closed so we perform a blocking take. We are guaranteed that additional
          // elements will be offered or the POISON will be added by close to unblock this thread.
          result = queue.take();
        }
      }
      if (result == POISON) {
        return null;
      }
      return result;
    }

    boolean isEmpty() {
      return queue.isEmpty() || queue.peek() == POISON;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(QueueingBeamFnDataClient.class);

  private final BeamFnDataClient mainClient;

  @GuardedBy("inboundDataClients")
  private final HashSet<InboundDataClient> inboundDataClients;

  @GuardedBy("inboundDataClients")
  private final ArrayList<InboundDataClient> finishedClients;

  @GuardedBy("inboundDataClients")
  private boolean isDraining = false;

  private final int queueSize;
  private ClosableQueue queue;

  public QueueingBeamFnDataClient(BeamFnDataClient mainClient, int queueSize) {
    this.mainClient = mainClient;
    this.inboundDataClients = new HashSet<>();
    this.finishedClients = new ArrayList<>();
    this.queueSize = queueSize;
    this.queue = new ClosableQueue(queueSize);
  }

  @Override
  public InboundDataClient receive(
      ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      FnDataReceiver<ByteString> consumer) {
    LOG.debug(
        "Registering consumer for instruction {} and transform {}",
        inputLocation.getInstructionId(),
        inputLocation.getTransformId());

    QueueingFnDataReceiver<ByteString> queueingConsumer =
        new QueueingFnDataReceiver<>(consumer, this.queue);
    InboundDataClient inboundDataClient =
        this.mainClient.receive(apiServiceDescriptor, inputLocation, queueingConsumer);
    queueingConsumer.inboundDataClient = inboundDataClient;
    synchronized (inboundDataClients) {
      Preconditions.checkState(!isDraining);
      if (this.inboundDataClients.add(inboundDataClient)) {
        inboundDataClient.runWhenComplete(() -> completeInbound(inboundDataClient));
      }
    }
    return inboundDataClient;
  }

  private void completeInbound(InboundDataClient client) {
    Preconditions.checkState(client.isDone());
    // This client will no longer be adding elements to the queue.
    //
    // There are several cases we consider here:
    // - this is not the last active client -> do nothing since the last client will handle things
    // - last client and we are draining -> we know that no additional elements will be added to the
    //   queue because there will be no more clients. We close the queue to trigger exiting
    //   drainAndBlock.
    // - last client and we are not draining -> it is possible that additional clients will be added
    //   with receive. drainAndBlock itself detects this case and close the queue.
    synchronized (inboundDataClients) {
      if (!inboundDataClients.remove(client)) {
        // Possible if this client was leftover from before reset() was called.
        return;
      }
      finishedClients.add(client);
      if (inboundDataClients.isEmpty() && isDraining) {
        queue.close();
      }
    }
  }

  /**
   * Drains the internal queue of this class, by waiting for all values to be passed to their
   * consumers. The thread which wishes to process() the elements should call this method, as this
   * will cause the consumers to invoke element processing. All receive() and send() calls must be
   * made prior to calling drainAndBlock, in order to properly terminate.
   *
   * <p>All {@link InboundDataClient}s will be failed if processing throws an exception.
   *
   * <p>This method is NOT thread safe. This should only be invoked once by a single thread. See
   * class comment.
   */
  public void drainAndBlock() throws Exception {
    // There are several ways drainAndBlock completes:
    // - processing elements fails -> all inbound clients are failed and exception thrown
    // - draining starts while inbound clients are active -> the last client will poision the queue
    //   to notify that no more elements will arrive
    // - draining starts without any remaining clients -> we just need to drain the queue and then
    //   are done as no further elements will arrive.
    synchronized (inboundDataClients) {
      Preconditions.checkState(!isDraining);
      isDraining = true;
      if (inboundDataClients.isEmpty()) {
        queue.close();
      }
    }
    while (true) {
      try {
        @Nullable ConsumerAndData<?> tuple = queue.take();
        if (tuple == null) {
          break; // queue has been drained and is closed.
        }

        // Forward to the consumers who cares about this data.
        tuple.accept();
      } catch (Exception e) {
        LOG.error("Client failed to deque and process the value", e);
        HashSet<InboundDataClient> clients = new HashSet<>();
        synchronized (inboundDataClients) {
          clients.addAll(inboundDataClients);
          clients.addAll(finishedClients);
        }
        for (InboundDataClient inboundDataClient : clients) {
          inboundDataClient.fail(e);
        }
        throw e;
      }
    }
    synchronized (inboundDataClients) {
      Preconditions.checkState(inboundDataClients.isEmpty());
      Preconditions.checkState(isDraining);
    }
    Preconditions.checkState(queue.isEmpty());
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

  /** Resets this object so that it may be reused. */
  public void reset() {
    synchronized (inboundDataClients) {
      inboundDataClients.clear();
      isDraining = false;
      finishedClients.clear();
    }
    // It is possible that previous inboundClients were failed but could still be adding
    // additional elements to their bound queue. For this reason we create a new queue.
    this.queue = new ClosableQueue(queueSize);
  }

  /**
   * The QueueingFnDataReceiver is a a FnDataReceiver used by the QueueingBeamFnDataClient.
   *
   * <p>All {@link #accept accept()ed} values will be put onto a synchronous queue which will cause
   * the calling thread to block until {@link QueueingBeamFnDataClient#drainAndBlock} is called or
   * the inboundClient is failed. {@link QueueingBeamFnDataClient#drainAndBlock} is responsible for
   * processing values from the queue.
   */
  private static class QueueingFnDataReceiver<T> implements FnDataReceiver<T> {
    private final FnDataReceiver<T> consumer;
    private final ClosableQueue queue;
    public @Nullable InboundDataClient inboundDataClient; // Null only during initialization.

    public QueueingFnDataReceiver(FnDataReceiver<T> consumer, ClosableQueue queue) {
      this.queue = queue;
      this.consumer = consumer;
    }

    /**
     * This method is thread safe, we expect multiple threads to call this, passing in data when new
     * data arrives via the QueueingBeamFnDataClient's mainClient.
     */
    @Override
    public void accept(T value) throws Exception {
      @SuppressWarnings("nullness")
      final @NonNull InboundDataClient client = this.inboundDataClient;
      try {
        ConsumerAndData<T> offering = new ConsumerAndData<>(this.consumer, value);
        while (!this.queue.offer(offering, 200, TimeUnit.MILLISECONDS)) {
          if (client.isDone()) {
            // If it was cancelled by the consuming side of the queue.
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to insert the value into the queue", e);
        client.fail(e);
        throw e;
      }
    }
  }

  private static class ConsumerAndData<T> {
    private final FnDataReceiver<T> consumer;
    private final T data;

    public ConsumerAndData(FnDataReceiver<T> receiver, T data) {
      this.consumer = receiver;
      this.data = data;
    }

    void accept() throws Exception {
      consumer.accept(data);
    }
  }
}
