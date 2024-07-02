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
package org.apache.beam.runners.dataflow.worker.windmill.work.refresh;

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.sdk.annotations.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HeartbeatSender} implementation that sends heartbeats directly on the underlying stream if
 * the stream is not closed.
 *
 * @implNote
 *     <p>{@link #equals(Object)} and {@link #hashCode()} implementations delegate to internal
 *     {@link GetDataStream} implementations so that requests can be grouped and sent on the same
 *     stream.
 *     <p>{@link #onStreamClosed} is a hook used to bind side effects if {@link
 *     #sendHeartbeats(Map)} is called when the underlying stream is closed, and defaults as a
 *     no-op.
 */
@Internal
public final class DirectHeartbeatSender implements HeartbeatSender {
  private static final Logger LOG = LoggerFactory.getLogger(DirectHeartbeatSender.class);
  private static final Runnable NO_OP_ON_STREAM_CLOSED_HANDLER = () -> {};
  private final GetDataStream getDataStream;
  private final Runnable onStreamClosed;

  private DirectHeartbeatSender(GetDataStream getDataStream, Runnable onStreamClosed) {
    this.getDataStream = getDataStream;
    this.onStreamClosed = onStreamClosed;
  }

  public static DirectHeartbeatSender create(GetDataStream getDataStream) {
    return new DirectHeartbeatSender(getDataStream, NO_OP_ON_STREAM_CLOSED_HANDLER);
  }

  @Override
  public void sendHeartbeats(Map<String, List<HeartbeatRequest>> heartbeats) {
    if (getDataStream.isShutdown() || getDataStream.isClosed()) {
      LOG.warn(
          "Trying to refresh work w/ {} heartbeats on stream={} after work has moved off of worker."
              + " heartbeats",
          getDataStream.backendWorkerToken(),
          heartbeats.size());
      onStreamClosed.run();
    } else {
      getDataStream.refreshActiveWork(heartbeats);
    }
  }

  public HeartbeatSender withStreamClosedHandler(Runnable onStreamClosed) {
    return new DirectHeartbeatSender(getDataStream, onStreamClosed);
  }

  public boolean hasStreamClosedHandler() {
    return !onStreamClosed.equals(NO_OP_ON_STREAM_CLOSED_HANDLER);
  }

  @Override
  public int hashCode() {
    return getDataStream.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DirectHeartbeatSender
        && getDataStream.equals(((DirectHeartbeatSender) obj).getDataStream);
  }
}
