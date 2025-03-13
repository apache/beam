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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.api.core.ApiFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListenableFuture;

/** Adapts {@link ListenableFuture} from bigtable-client-core to vendored guava. */
class VendoredListenableFutureAdapter<V> implements ListenableFuture<V> {

  private final ApiFuture<Void> underlying;

  VendoredListenableFutureAdapter(ApiFuture<Void> underlying) {
    this.underlying = underlying;
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    underlying.addListener(listener, executor);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return underlying.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return underlying.isCancelled();
  }

  @Override
  public boolean isDone() {
    return underlying.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return (V) underlying.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return (V) underlying.get(timeout, unit);
  }
}
