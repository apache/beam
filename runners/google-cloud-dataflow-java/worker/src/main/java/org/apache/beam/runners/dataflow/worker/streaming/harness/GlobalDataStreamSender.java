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

import java.io.Closeable;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;

@Internal
@ThreadSafe
// TODO (m-trieu): replace Supplier<Stream> with Stream after github.com/apache/beam/pull/32774/ is
// merged
final class GlobalDataStreamSender implements Closeable, Supplier<GetDataStream> {
  private final Endpoint endpoint;
  private final Supplier<GetDataStream> delegate;
  private volatile boolean started;

  GlobalDataStreamSender(Supplier<GetDataStream> delegate, Endpoint endpoint) {
    // Ensures that the Supplier is thread-safe
    this.delegate = Suppliers.memoize(delegate::get);
    this.started = false;
    this.endpoint = endpoint;
  }

  @Override
  public GetDataStream get() {
    if (!started) {
      started = true;
    }
    return delegate.get();
  }

  @Override
  public void close() {
    if (started) {
      delegate.get().shutdown();
    }
  }

  Endpoint endpoint() {
    return endpoint;
  }
}
