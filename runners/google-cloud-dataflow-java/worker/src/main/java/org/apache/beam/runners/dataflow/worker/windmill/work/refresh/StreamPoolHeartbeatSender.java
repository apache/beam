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

import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.sdk.annotations.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** StreamingEngine stream pool based implementation of {@link HeartbeatSender}. */
@Internal
public final class StreamPoolHeartbeatSender implements HeartbeatSender {
  private static final Logger LOG = LoggerFactory.getLogger(StreamPoolHeartbeatSender.class);

  private final WindmillStreamPool<WindmillStream.GetDataStream> heartbeatStreamPool;

  public StreamPoolHeartbeatSender(
      WindmillStreamPool<WindmillStream.GetDataStream> heartbeatStreamPool) {
    this.heartbeatStreamPool = heartbeatStreamPool;
  }

  @Override
  public void sendHeartbeats(Heartbeats heartbeats) {
    try (CloseableStream<WindmillStream.GetDataStream> closeableStream =
        heartbeatStreamPool.getCloseableStream()) {
      closeableStream.stream().refreshActiveWork(heartbeats.heartbeatRequests().asMap());
    } catch (Exception e) {
      LOG.warn("Error occurred sending heartbeats=[{}].", heartbeats, e);
    }
  }
}
