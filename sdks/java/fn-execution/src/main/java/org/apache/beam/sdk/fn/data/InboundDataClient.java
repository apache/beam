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

import java.util.concurrent.CancellationException;

/**
 * A client representing some stream of inbound data. An inbound data client can be completed or
 * cancelled, in which case it will ignore any future inputs. It can also be awaited on.
 */
public interface InboundDataClient {
  /**
   * Block until the client has completed reading from the inbound stream.
   *
   * @throws InterruptedException if the client is interrupted before completing.
   * @throws CancellationException if the client is cancelled before completing.
   * @throws Exception if the client throws an exception while awaiting completion.
   */
  void awaitCompletion() throws InterruptedException, Exception;

  /**
   * Returns true if the client is done, either via completing successfully or by being cancelled.
   */
  boolean isDone();

  /** Cancels the client, causing it to drop any future inbound data. */
  void cancel();

  /** Mark the client as completed. */
  void complete();

  /**
   * Mark the client as completed with an exception. Calls to awaitCompletion will terminate by
   * throwing the provided exception.
   *
   * @param t the throwable that caused this client to fail
   */
  void fail(Throwable t);
}
