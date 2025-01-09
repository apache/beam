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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers;

import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Internal;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;

@Internal
public interface TerminatingStreamObserver<T> extends StreamObserver<T> {

  /**
   * Terminates the StreamObserver.
   *
   * @implSpec Different then {@link #onError(Throwable)} and {@link #onCompleted()} which can only
   *     be called once during the lifetime of each {@link StreamObserver}, terminate()
   *     implementations are meant to be idempotent and can be called multiple times as well as
   *     being interleaved with other stream operations.
   */
  void terminate(Throwable terminationException);
}
