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

import static org.junit.Assert.assertEquals;

import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.FakeWindmillServer;
import org.apache.beam.runners.dataflow.worker.streaming.config.FakeGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerRunnerV1Settings;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamPoolHeartbeatSenderTest {

  @Test
  public void sendsHeartbeatsOnStream() {
    FakeWindmillServer server = new FakeWindmillServer(new ErrorCollector(), c -> Optional.empty());
    StreamPoolHeartbeatSender heartbeatSender =
        StreamPoolHeartbeatSender.create(
            WindmillStreamPool.create(1, Duration.standardSeconds(10), server::getDataStream));
    Heartbeats.Builder heartbeatsBuilder = Heartbeats.builder();
    heartbeatsBuilder
        .heartbeatRequestsBuilder()
        .put("key", HeartbeatRequest.newBuilder().setWorkToken(123).build());
    heartbeatSender.sendHeartbeats(heartbeatsBuilder.build());
    assertEquals(1, server.getGetDataRequests().size());
  }

  @Test
  public void sendsHeartbeatsOnDedicatedStream() {
    FakeWindmillServer dedicatedServer =
        new FakeWindmillServer(new ErrorCollector(), c -> Optional.empty());
    FakeWindmillServer getDataServer =
        new FakeWindmillServer(new ErrorCollector(), c -> Optional.empty());

    FakeGlobalConfigHandle configHandle =
        new FakeGlobalConfigHandle(getGlobalConfig(/*useSeparateHeartbeatStreams=*/ true));
    StreamPoolHeartbeatSender heartbeatSender =
        StreamPoolHeartbeatSender.create(
            WindmillStreamPool.create(
                1, Duration.standardSeconds(10), dedicatedServer::getDataStream),
            WindmillStreamPool.create(
                1, Duration.standardSeconds(10), getDataServer::getDataStream),
            configHandle);
    Heartbeats.Builder heartbeatsBuilder = Heartbeats.builder();
    heartbeatsBuilder
        .heartbeatRequestsBuilder()
        .put("key", HeartbeatRequest.newBuilder().setWorkToken(123).build());
    heartbeatSender.sendHeartbeats(heartbeatsBuilder.build());
    assertEquals(1, dedicatedServer.getGetDataRequests().size());
    assertEquals(0, getDataServer.getGetDataRequests().size());

    heartbeatSender.sendHeartbeats(heartbeatsBuilder.build());
    assertEquals(2, dedicatedServer.getGetDataRequests().size());
    assertEquals(0, getDataServer.getGetDataRequests().size());

    // Turn off separate heartbeats
    configHandle.setConfig(getGlobalConfig(/*useSeparateHeartbeatStreams=*/ false));
    heartbeatSender.sendHeartbeats(heartbeatsBuilder.build());
    // request to getDataServer increases and dedicatedServer remains same
    assertEquals(2, dedicatedServer.getGetDataRequests().size());
    assertEquals(1, getDataServer.getGetDataRequests().size());
  }

  private static StreamingGlobalConfig getGlobalConfig(boolean useSeparateHeartbeatStreams) {
    return StreamingGlobalConfig.builder()
        .setUserWorkerJobSettings(
            UserWorkerRunnerV1Settings.newBuilder()
                .setUseSeparateWindmillHeartbeatStreams(useSeparateHeartbeatStreams)
                .build())
        .build();
  }

  @Test
  public void sendsHeartbeatsOnGetDataStream() {
    FakeWindmillServer dedicatedServer =
        new FakeWindmillServer(new ErrorCollector(), c -> Optional.empty());
    FakeWindmillServer getDataServer =
        new FakeWindmillServer(new ErrorCollector(), c -> Optional.empty());

    FakeGlobalConfigHandle configHandle =
        new FakeGlobalConfigHandle(getGlobalConfig(/*useSeparateHeartbeatStreams=*/ false));
    StreamPoolHeartbeatSender heartbeatSender =
        StreamPoolHeartbeatSender.create(
            WindmillStreamPool.create(
                1, Duration.standardSeconds(10), dedicatedServer::getDataStream),
            WindmillStreamPool.create(
                1, Duration.standardSeconds(10), getDataServer::getDataStream),
            configHandle);
    Heartbeats.Builder heartbeatsBuilder = Heartbeats.builder();
    heartbeatsBuilder
        .heartbeatRequestsBuilder()
        .put("key", HeartbeatRequest.newBuilder().setWorkToken(123).build());
    heartbeatSender.sendHeartbeats(heartbeatsBuilder.build());
    assertEquals(0, dedicatedServer.getGetDataRequests().size());
    assertEquals(1, getDataServer.getGetDataRequests().size());

    heartbeatSender.sendHeartbeats(heartbeatsBuilder.build());
    assertEquals(0, dedicatedServer.getGetDataRequests().size());
    assertEquals(2, getDataServer.getGetDataRequests().size());

    // Turn on separate heartbeats
    configHandle.setConfig(getGlobalConfig(/*useSeparateHeartbeatStreams=*/ true));
    heartbeatSender.sendHeartbeats(heartbeatsBuilder.build());
    // request to dedicatedServer increases and getDataServer remains same
    assertEquals(1, dedicatedServer.getGetDataRequests().size());
    assertEquals(2, getDataServer.getGetDataRequests().size());
  }
}
