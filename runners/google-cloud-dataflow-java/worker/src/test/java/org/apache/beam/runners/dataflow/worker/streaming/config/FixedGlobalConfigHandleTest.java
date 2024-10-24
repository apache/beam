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
package org.apache.beam.runners.dataflow.worker.streaming.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.dataflow.worker.OperationalLimits;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerRunnerV1Settings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FixedGlobalConfigHandleTest {

  @Test
  public void getConfig() {
    StreamingGlobalConfig config =
        StreamingGlobalConfig.builder()
            .setOperationalLimits(
                OperationalLimits.builder()
                    .setMaxOutputValueBytes(123)
                    .setMaxOutputKeyBytes(324)
                    .setMaxWorkItemCommitBytes(456)
                    .build())
            .setWindmillServiceEndpoints(ImmutableSet.of(HostAndPort.fromHost("windmillHost")))
            .setUserWorkerJobSettings(
                UserWorkerRunnerV1Settings.newBuilder()
                    .setUseSeparateWindmillHeartbeatStreams(false)
                    .build())
            .build();
    FixedGlobalConfigHandle globalConfigHandle = new FixedGlobalConfigHandle(config);
    assertEquals(config, globalConfigHandle.getConfig());
  }

  @Test
  public void registerConfigObserver() throws InterruptedException {
    StreamingGlobalConfig config =
        StreamingGlobalConfig.builder()
            .setOperationalLimits(
                OperationalLimits.builder()
                    .setMaxOutputValueBytes(123)
                    .setMaxOutputKeyBytes(324)
                    .setMaxWorkItemCommitBytes(456)
                    .build())
            .setWindmillServiceEndpoints(ImmutableSet.of(HostAndPort.fromHost("windmillHost")))
            .setUserWorkerJobSettings(
                UserWorkerRunnerV1Settings.newBuilder()
                    .setUseSeparateWindmillHeartbeatStreams(false)
                    .build())
            .build();
    FixedGlobalConfigHandle globalConfigHandle = new FixedGlobalConfigHandle(config);
    AtomicReference<StreamingGlobalConfig> configFromCallback = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    globalConfigHandle.registerConfigObserver(
        cbConfig -> {
          configFromCallback.set(cbConfig);
          latch.countDown();
        });
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(configFromCallback.get(), globalConfigHandle.getConfig());
  }
}
