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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import java.util.function.Supplier;
import org.joda.time.Duration;

/**
 * Closeable {@link WindmillStream} factory that uses a {@link WindmillStreamPool} to create and
 * release streams.
 */
public final class WindmillStreamPoolCloseableStreamFactory<StreamT extends WindmillStream>
    implements Supplier<CloseableStream<StreamT>> {
  private static final int NUM_COMMIT_STREAMS = 1;
  private static final Duration COMMIT_STREAM_TIMEOUT = Duration.standardMinutes(1);

  private final WindmillStreamPool<StreamT> streamPool;

  public WindmillStreamPoolCloseableStreamFactory(Supplier<StreamT> streamFactory) {
    this.streamPool =
        WindmillStreamPool.create(NUM_COMMIT_STREAMS, COMMIT_STREAM_TIMEOUT, streamFactory);
  }

  @Override
  public CloseableStream<StreamT> get() {
    StreamT stream = streamPool.getStream();
    return CloseableStream.create(() -> stream, () -> streamPool.releaseStream(stream));
  }
}
