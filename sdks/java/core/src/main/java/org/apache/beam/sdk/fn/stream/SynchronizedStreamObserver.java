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
package org.apache.beam.sdk.fn.stream;

import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/**
 * A {@link StreamObserver} which provides synchronous access access to an underlying {@link
 * StreamObserver}.
 *
 * <p>The underlying {@link StreamObserver} must not be used by any other clients.
 */
public class SynchronizedStreamObserver<V> implements StreamObserver<V> {
  private final StreamObserver<V> underlying;

  private SynchronizedStreamObserver(StreamObserver<V> underlying) {
    this.underlying = underlying;
  }

  /**
   * Create a new {@link SynchronizedStreamObserver} which will delegate all calls to the underlying
   * {@link StreamObserver}, synchronizing access to that observer.
   */
  public static <V> StreamObserver<V> wrapping(StreamObserver<V> underlying) {
    return new SynchronizedStreamObserver<>(underlying);
  }

  @Override
  public void onNext(V value) {
    synchronized (underlying) {
      underlying.onNext(value);
    }
  }

  @Override
  public synchronized void onError(Throwable t) {
    synchronized (underlying) {
      underlying.onError(t);
    }
  }

  @Override
  public synchronized void onCompleted() {
    synchronized (underlying) {
      underlying.onCompleted();
    }
  }
}
