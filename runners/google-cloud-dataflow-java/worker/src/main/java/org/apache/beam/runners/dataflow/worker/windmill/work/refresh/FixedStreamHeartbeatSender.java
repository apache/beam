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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.streaming.RefreshableWork;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
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
 *     stream instance.
 *     <p>This class is a stateless decorator to the underlying stream.
 */
@Internal
public final class FixedStreamHeartbeatSender implements HeartbeatSender {
  private static final Logger LOG = LoggerFactory.getLogger(FixedStreamHeartbeatSender.class);
  private final GetDataStream getDataStream;

  private FixedStreamHeartbeatSender(GetDataStream getDataStream) {
    this.getDataStream = getDataStream;
  }

  public static FixedStreamHeartbeatSender create(GetDataStream getDataStream) {
    return new FixedStreamHeartbeatSender(getDataStream);
  }

  @Override
  public void sendHeartbeats(Heartbeats heartbeats) {
    @Nullable String originalThreadName = null;
    try {
      String backendWorkerToken = getDataStream.backendWorkerToken();
      if (!backendWorkerToken.isEmpty()) {
        // Decorate the thread name w/ the backendWorkerToken for debugging. Resets the thread's
        // name after sending the heartbeats succeeds or fails.
        originalThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(originalThreadName + "-" + backendWorkerToken);
      }
      getDataStream.refreshActiveWork(heartbeats.heartbeatRequests().asMap());
    } catch (AbstractWindmillStream.WindmillStreamShutdownException e) {
      LOG.debug(
          "Trying to send {} heartbeats to worker=[{}] after work has moved off of worker.",
          getDataStream.backendWorkerToken(),
          heartbeats.heartbeatRequests().size());
      heartbeats.work().forEach(RefreshableWork::setFailed);
    } finally {
      if (originalThreadName != null) {
        Thread.currentThread().setName(originalThreadName);
      }
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(FixedStreamHeartbeatSender.class, getDataStream);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof FixedStreamHeartbeatSender
        && getDataStream.equals(((FixedStreamHeartbeatSender) obj).getDataStream);
  }

  @Override
  public String toString() {
    return "HeartbeatSender-" + getDataStream.backendWorkerToken();
  }
}
