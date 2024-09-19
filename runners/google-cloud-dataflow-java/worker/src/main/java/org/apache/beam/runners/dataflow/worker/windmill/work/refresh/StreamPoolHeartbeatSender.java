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

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
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

  @Nonnull
  private final AtomicReference<WindmillStreamPool<WindmillStream.GetDataStream>>
      heartbeatStreamPool = new AtomicReference<>();

  private StreamPoolHeartbeatSender(
      WindmillStreamPool<WindmillStream.GetDataStream> heartbeatStreamPool) {
    this.heartbeatStreamPool.set(heartbeatStreamPool);
  }

  public static StreamPoolHeartbeatSender Create(
      @Nonnull WindmillStreamPool<WindmillStream.GetDataStream> heartbeatStreamPool) {
    return new StreamPoolHeartbeatSender(heartbeatStreamPool);
  }

  /**
   * Creates StreamPoolHeartbeatSender that switches between the passed in stream pools depending on
   * global config.
   *
   * @param heartbeatStreamPool stream to use when using separate streams for heartbeat is enabled.
   * @param getDataPool stream to use when using separate streams for heartbeat is disabled.
   */
  public static StreamPoolHeartbeatSender Create(
      @Nonnull WindmillStreamPool<WindmillStream.GetDataStream> heartbeatStreamPool,
      @Nonnull WindmillStreamPool<WindmillStream.GetDataStream> getDataPool,
      @Nonnull StreamingGlobalConfigHandle configHandle) {
    // Use getDataPool as the default, settings callback will
    // switch to the separate pool if enabled before processing any elements are processed.
    StreamPoolHeartbeatSender heartbeatSender = new StreamPoolHeartbeatSender(heartbeatStreamPool);
    configHandle.registerConfigObserver(
        streamingGlobalConfig ->
            heartbeatSender.heartbeatStreamPool.set(
                streamingGlobalConfig
                        .userWorkerJobSettings()
                        .getUseSeparateWindmillHeartbeatStreams()
                    ? heartbeatStreamPool
                    : getDataPool));
    return heartbeatSender;
  }

  @Override
  public void sendHeartbeats(Heartbeats heartbeats) {
    try (CloseableStream<WindmillStream.GetDataStream> closeableStream =
        heartbeatStreamPool.get().getCloseableStream()) {
      closeableStream.stream().refreshActiveWork(heartbeats.heartbeatRequests().asMap());
    } catch (Exception e) {
      LOG.warn("Error occurred sending heartbeats=[{}].", heartbeats, e);
    }
  }
}
