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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.sdk.annotations.Internal;

@Internal
@ThreadSafe
final class GlobalDataStreamSender implements StreamSender {
  private final Endpoint endpoint;
  private final GetDataStream delegate;
  private volatile boolean started;

  GlobalDataStreamSender(GetDataStream delegate, Endpoint endpoint) {
    this.delegate = delegate;
    this.started = false;
    this.endpoint = endpoint;
  }

  GetDataStream stream() {
    if (!started) {
      // Starting the stream possibly perform IO. Start the stream lazily since not all pipeline
      // implementations need to fetch global/side input data.
      startStream();
    }

    return delegate;
  }

  private synchronized void startStream() {
    // Check started again after we acquire the lock.
    if (!started) {
      delegate.start();
      started = true;
    }
  }

  @Override
  public void close() {
    delegate.shutdown();
  }

  Endpoint endpoint() {
    return endpoint;
  }
}
