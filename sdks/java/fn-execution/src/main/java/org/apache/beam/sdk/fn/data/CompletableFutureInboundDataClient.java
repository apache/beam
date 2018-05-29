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

import java.util.concurrent.CompletableFuture;

/**
 * An {@link InboundDataClient} backed by a {@link CompletableFuture}.
 */
public class CompletableFutureInboundDataClient implements InboundDataClient {
  private static final Object COMPLETED = new Object();
  /**
   * Create a new {@link CompletableFutureInboundDataClient} using a new {@link CompletableFuture}.
   */
  public static InboundDataClient create() {
    return forBackingFuture(new CompletableFuture<>());
  }

  /**
   * Create a new {@link CompletableFutureInboundDataClient} wrapping the provided
   * {@link CompletableFuture}.
   */
  static InboundDataClient forBackingFuture(CompletableFuture<Object> future) {
    return new CompletableFutureInboundDataClient(future);
  }

  private final CompletableFuture<Object> future;

  private CompletableFutureInboundDataClient(CompletableFuture<Object> future) {
    this.future = future;
  }

  @Override
  public void awaitCompletion() throws Exception {
    future.get();
  }

  @Override
  public boolean isDone() {
    return future.isDone();
  }

  @Override
  public void cancel() {
    future.cancel(true);
  }

  @Override
  public void complete() {
    future.complete(COMPLETED);
  }

  @Override
  public void fail(Throwable t) {
    future.completeExceptionally(t);
  }
}
