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

import com.google.rpc.Code;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchWriteWithSummary;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.joda.time.Instant;

/**
 * Quality of Service manager for Firestore RPCs.
 *
 * <p>Cloud Firestore has a number of considerations for interacting with the database in a reliable
 * manner.
 *
 * <p>Every RPC which is sent to Cloud Firestore is subject to QoS considerations. Successful,
 * failed, attempted requests are all tracked and directly drive the determination of when to
 * attempt an RPC. In the case of a write rpc, the QoS will also determine the size of the request
 * in order to try and maximize throughput with success rate.
 *
 * <p>The lifecycle of an instance of {@link RpcQos} is expected to be bound to the lifetime of the
 * worker the RPC Functions run on. Explicitly, this instance should live longer than an individual
 * bundle.
 *
 * <p>Each request processed via one of the {@link org.apache.beam.sdk.transforms.PTransform}s
 * available in {@link FirestoreV1} will work its way through a state machine provided by this
 * {@link RpcQos}. The high level state machine events are as follows:
 *
 * <ol>
 *   <li>Create new {@link RpcAttempt}
 *   <li>Check if it is safe to proceed with sending the request ({@link
 *       RpcAttempt#awaitSafeToProceed(Instant)})
 *   <li>Record start of trying to send request
 *   <li>Record success or failure state of send request attempt
 *   <li>If success output all returned responses
 *   <li>If failure check retry ability ({@link RpcAttempt#checkCanRetry(Instant,
 *       RuntimeException)})
 *       <ol style="margin-top: 0">
 *         <li>Ensure the request has budget to retry ({@link RpcQosOptions#getMaxAttempts()})
 *         <li>Ensure the error is not a non-retryable error
 *       </ol>
 * </ol>
 *
 * Configuration of options can be accomplished by passing an instances of {@link RpcQosOptions} to
 * the {@code withRpcQosOptions} method of each {@code Builder} available in {@link FirestoreV1}.
 *
 * <p>A new instance of {@link RpcQosOptions.Builder} can be created via {@link
 * RpcQosOptions#newBuilder()}. A default instance of {@link RpcQosOptions} can be created via
 * {@link RpcQosOptions#defaultOptions()}.
 *
 * <p>
 *
 * @see FirestoreV1
 * @see FirestoreV1.BatchGetDocuments.Builder#withRpcQosOptions(RpcQosOptions)
 * @see BatchWriteWithSummary.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.ListCollectionIds.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.ListDocuments.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.PartitionQuery.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.RunQuery.Builder#withRpcQosOptions(RpcQosOptions)
 * @see <a target="_blank" rel="noopener noreferrer"
 *     href="https://cloud.google.com/firestore/quotas#limits">Standard limits</a>
 * @see <a target="_blank" rel="noopener noreferrer"
 *     href="https://cloud.google.com/firestore/docs/best-practices#designing_for_scale">Designing
 *     for scale</a>
 */
interface RpcQos {

  /**
   * Create a new stateful attempt for a read operation. The returned {@link RpcReadAttempt} will be
   * used for the full lifetime of trying to successfully process a request.
   *
   * @param context The {@link Context} which this new attempt should be associated with.
   * @return A new {@link RpcReadAttempt} which will be used while trying to successfully a request
   */
  RpcReadAttempt newReadAttempt(Context context);

  /**
   * Create a new stateful attempt for a write operation. The returned {@link RpcWriteAttempt} will
   * be used for the full lifetime of trying to successfully process a request.
   *
   * @param context The {@link Context} which this new attempt should be associated with.
   * @return A new {@link RpcWriteAttempt} which will be used while trying to successfully a request
   */
  RpcWriteAttempt newWriteAttempt(Context context);

  /**
   * Check if a request is over the max allowed number of bytes.
   *
   * @param bytes number of bytes to check against the allowed limit
   * @return true if {@code bytes} is over the allowed limit, false otherwise
   * @see RpcQosOptions#getBatchMaxBytes()
   */
  boolean bytesOverLimit(long bytes);

  /**
   * Base interface representing the lifespan of attempting to successfully process a single
   * request.
   */
  interface RpcAttempt {

    /**
     * Await until it is safe to proceed sending the rpc, evaluated relative to {@code start}. If it
     * is not yet safe to proceed, this method will block until it is safe to proceed.
     *
     * @param start The intended start time of the next rpc
     * @return true if it is safe to proceed with sending the next rpc, false otherwise.
     * @throws InterruptedException if this thread is interrupted while waiting
     * @see Thread#sleep(long)
     * @see org.apache.beam.sdk.util.Sleeper#sleep(long)
     */
    boolean awaitSafeToProceed(Instant start) throws InterruptedException;

    /**
     * Determine if an rpc can be retried given {@code instant} and {@code exception}.
     *
     * <p>If a backoff is necessary before retrying this method can block for backoff before
     * returning.
     *
     * <p>If no retry is available this {@link RpcAttempt} will move to a terminal failed state and
     * will error if further interaction is attempted.
     *
     * @param instant The instant with which to evaluate retryability
     * @param exception Exception to evaluate for retry ability
     * @throws InterruptedException if this thread is interrupted while waiting
     */
    void checkCanRetry(Instant instant, RuntimeException exception) throws InterruptedException;

    /**
     * Mark this {@link RpcAttempt} as having completed successfully, moving to a terminal success
     * state. If any further interaction is attempted an error will be thrown.
     */
    void completeSuccess();

    boolean isCodeRetryable(Code code);

    void recordRequestSuccessful(Instant end);

    void recordRequestFailed(Instant end);

    /**
     * Context which an attempt should be associated with.
     *
     * <p>Some things which are associated with an attempt:
     *
     * <ol>
     *   <li>Log appender
     *   <li>Metrics
     * </ol>
     */
    interface Context {

      /**
       * The namespace used for log appender and metrics.
       *
       * @return the namespace to use
       */
      String getNamespace();
    }
  }

  /**
   * Read specific interface for {@link RpcAttempt}.
   *
   * <p>This interface provides those methods which apply to read operations and the state tracked
   * related to read operations.
   */
  interface RpcReadAttempt extends RpcAttempt {

    /** Record the start time of sending the rpc. */
    void recordRequestStart(Instant start);

    void recordStreamValue(Instant now);
  }

  /**
   * Write specific interface for {@link RpcAttempt}.
   *
   * <p>This interface provides those methods which apply to write operations and the state tracked
   * related to write operations.
   */
  interface RpcWriteAttempt extends RpcAttempt {

    /**
     * Create a new {@link FlushBuffer} that can be sized relative to the QoS state and to the
     * provided {@code instant}.
     *
     * @param instant The intended start time of the next rpc
     * @param <ElementT> The {@link Element} type which the returned buffer will contain
     * @return a new {@link FlushBuffer} which queued messages can be staged to before final flush
     */
    <ElementT extends Element<?>> FlushBuffer<ElementT> newFlushBuffer(Instant instant);

    /** Record the start time of sending the rpc. */
    void recordRequestStart(Instant start, int numWrites);

    void recordWriteCounts(Instant end, int successfulWrites, int failedWrites);

    /**
     * A buffer which is sized related to QoS state and provides a staging location for elements
     * before a request is actually created and sent.
     *
     * @param <ElementT> The {@link Element} type which will be stored in this instance
     */
    interface FlushBuffer<ElementT extends Element<?>> extends Iterable<ElementT> {
      /**
       * Attempt to add {@code newElement} to this {@link FlushBuffer}.
       *
       * @param newElement The {@link Element} to try and add
       * @return true if the flush group has capacity for newElement, false otherwise
       */
      boolean offer(ElementT newElement);

      /** @return the number of elements that are currently buffered in this instance */
      int getBufferedElementsCount();

      /** @return the number of bytes that are currently buffered in this instance */
      long getBufferedElementsBytes();

      /**
       * @return true if the buffer contains enough {@link Element}s such that a flush should
       *     happen, false otherwise
       */
      boolean isFull();

      boolean isNonEmpty();
    }

    /**
     * An element which can be added to a {@link FlushBuffer}. This interface is mainly a marker for
     * ensuring an encapsulated lifecycle managed way of determining the serialized size of {@code
     * T} which will need to be sent in a request.
     *
     * <p>Due to the nature of how protobufs handle serialization, we want to ensure we only
     * calculate the serialization size once so we avoid unnecessary memory pressure. The serialized
     * size is read many times through out the lifecycle of determining to send a T.
     *
     * @param <T> The type which will sent in the request
     */
    interface Element<T> {

      /** @return the value that will be sent in a request */
      T getValue();

      /**
       * This method should memoize the calculated size to avoid unnecessary memory pressure due to
       * this method being called many times over the course of its lifecycle.
       *
       * @return the number of bytes T is when serialized
       */
      long getSerializedSize();
    }
  }
}
