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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.sdk.annotations.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HeartbeatSender} implementation that sends heartbeats directly on the underlying stream.
 * If the stream is closed, does nothing.
 *
 * @implNote {@link #equals(Object)} and {@link #hashCode()} implementations delegate to internal
 *     {@link org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream}
 *     implementations so that requests can be grouped and sent on the same stream.
 */
@Internal
public final class DirectHeartbeatSender implements HeartbeatSender {
  private static final Logger LOG = LoggerFactory.getLogger(DirectHeartbeatSender.class);
  private final WindmillStream.GetDataStream getDataStream;

  public DirectHeartbeatSender(WindmillStream.GetDataStream getDataStream) {
    this.getDataStream = getDataStream;
  }

  @Override
  public void sendHeartbeats(Map<String, List<Windmill.HeartbeatRequest>> heartbeats) {
    if (isInvalid()) {
      LOG.warn(
          "Trying to refresh work on stream={} after work has moved off of worker."
              + " heartbeats={}",
          getDataStream,
          heartbeats);
    } else {
      getDataStream.refreshActiveWork(heartbeats);
    }
  }

  @Override
  public synchronized boolean isInvalid() {
    return getDataStream.isClosed();
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
