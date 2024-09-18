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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.OperationalLimits;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerRunnerV1Settings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingGlobalConfigHandleImplTest {

  @Test
  public void getConfig() {
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
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
    globalConfigHandle.setConfig(config);
    assertEquals(config, globalConfigHandle.getConfig());
  }

  @Test
  public void registerConfigObserver_configSetAfterRegisteringCallback()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    StreamingGlobalConfig configToSet =
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
    AtomicReference<StreamingGlobalConfig> configFromCallback1 = new AtomicReference<>();
    AtomicReference<StreamingGlobalConfig> configFromCallback2 = new AtomicReference<>();
    globalConfigHandle.registerConfigObserver(
        config -> {
          configFromCallback1.set(config);
          latch.countDown();
        });
    globalConfigHandle.registerConfigObserver(
        config -> {
          configFromCallback2.set(config);
          latch.countDown();
        });
    globalConfigHandle.setConfig(configToSet);
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(configFromCallback1.get(), globalConfigHandle.getConfig());
    assertEquals(configFromCallback2.get(), globalConfigHandle.getConfig());
  }

  @Test
  public void registerConfigObserver_configSetBeforeRegisteringCallback()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    StreamingGlobalConfig configToSet =
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
    AtomicReference<StreamingGlobalConfig> configFromCallback1 = new AtomicReference<>();
    AtomicReference<StreamingGlobalConfig> configFromCallback2 = new AtomicReference<>();
    globalConfigHandle.setConfig(configToSet);
    globalConfigHandle.registerConfigObserver(
        config -> {
          configFromCallback1.set(config);
          latch.countDown();
        });
    globalConfigHandle.registerConfigObserver(
        config -> {
          configFromCallback2.set(config);
          latch.countDown();
        });
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(configFromCallback1.get(), globalConfigHandle.getConfig());
    assertEquals(configFromCallback2.get(), globalConfigHandle.getConfig());
  }

  @Test
  public void registerConfigObserver_configSetBeforeRegisteringCallback_callbackThrowsException()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    StreamingGlobalConfig configToSet =
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
    AtomicReference<StreamingGlobalConfig> configFromCallback = new AtomicReference<>();
    globalConfigHandle.setConfig(configToSet);
    globalConfigHandle.registerConfigObserver(
        config -> {
          latch.countDown();
          throw new RuntimeException();
        });
    globalConfigHandle.registerConfigObserver(
        config -> {
          configFromCallback.set(config);
          latch.countDown();
        });
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(configFromCallback.get(), configToSet);
  }

  @Test
  public void registerConfigObserver_configSetAfterRegisteringCallback_callbackThrowsException()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    StreamingGlobalConfig configToSet =
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
    AtomicReference<StreamingGlobalConfig> configFromCallback = new AtomicReference<>();
    globalConfigHandle.registerConfigObserver(
        config -> {
          latch.countDown();
          throw new RuntimeException();
        });
    globalConfigHandle.registerConfigObserver(
        config -> {
          configFromCallback.set(config);
          latch.countDown();
        });
    globalConfigHandle.setConfig(configToSet);
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(configFromCallback.get(), configToSet);
  }

  @Test
  public void registerConfigObserver_shouldNotCallCallbackForIfConfigRemainsSame()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callbackCount = new AtomicInteger(0);
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    Supplier<StreamingGlobalConfig> configToSet =
        () ->
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
    globalConfigHandle.registerConfigObserver(
        config -> {
          callbackCount.incrementAndGet();
          latch.countDown();
        });
    globalConfigHandle.setConfig(configToSet.get());
    // call setter again with same config
    globalConfigHandle.setConfig(configToSet.get());
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
    assertEquals(1, callbackCount.get());
  }

  @Test
  public void registerConfigObserver_updateConfigWhenCallbackIsRunning()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    StreamingGlobalConfig initialConfig =
        StreamingGlobalConfig.builder()
            .setOperationalLimits(OperationalLimits.builder().setMaxOutputValueBytes(4569).build())
            .build();
    StreamingGlobalConfig updatedConfig =
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
    CopyOnWriteArrayList<StreamingGlobalConfig> configsFromCallback = new CopyOnWriteArrayList<>();
    globalConfigHandle.registerConfigObserver(
        config -> {
          configsFromCallback.add(config);
          if (globalConfigHandle.getConfig().equals(config)) {
            globalConfigHandle.setConfig(updatedConfig);
          }
          latch.countDown();
        });
    globalConfigHandle.setConfig(initialConfig);
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(configsFromCallback.get(0), initialConfig);
    assertEquals(configsFromCallback.get(1), updatedConfig);
  }
}
