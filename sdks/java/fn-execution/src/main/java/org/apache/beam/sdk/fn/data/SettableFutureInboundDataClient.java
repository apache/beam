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

import com.google.common.util.concurrent.SettableFuture;

/** An {@link InboundDataClient} backed by a {@link SettableFuture}. */
class SettableFutureInboundDataClient implements InboundDataClient {
  public static InboundDataClient create() {
    return createWithBackingFuture(SettableFuture.<Void>create());
  }

  static InboundDataClient createWithBackingFuture(SettableFuture<Void> future) {
    return new SettableFutureInboundDataClient(future);
  }

  private final SettableFuture<Void> future;

  private SettableFutureInboundDataClient(SettableFuture<Void> future) {
    this.future = future;
  }

  @Override
  public void awaitCompletion() throws InterruptedException, Exception {
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
    future.set(null);
  }

  @Override
  public void fail(Throwable t) {
    future.setException(t);
  }
}
