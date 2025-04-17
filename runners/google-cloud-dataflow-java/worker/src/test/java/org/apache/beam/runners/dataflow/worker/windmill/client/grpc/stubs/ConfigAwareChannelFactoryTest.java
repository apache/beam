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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConfigAwareChannelFactoryTest {
  private static final Windmill.UserWorkerGrpcFlowControlSettings DEFAULT_FLOW_CONTROL_SETTINGS =
      Windmill.UserWorkerGrpcFlowControlSettings.getDefaultInstance();
  private static final WindmillServiceAddress DEFAULT_SERVICE_ADDRESS =
      WindmillServiceAddress.create(HostAndPort.fromHost("www.google.com"));
  private ConfigAwareChannelFactory channelFactory;

  @Before
  public void setUp() {
    channelFactory = new ConfigAwareChannelFactory(0);
  }

  @Test
  public void testCreate_noInternalConfig() {
    ManagedChannel channel =
        channelFactory.create(DEFAULT_FLOW_CONTROL_SETTINGS, DEFAULT_SERVICE_ADDRESS);
    assertThat(channel).isNotInstanceOf(IsolationChannel.class);
  }

  @Test
  public void testCreate_isolationChannelEnabled() {
    channelFactory.tryConsumeJobConfig(
        StreamingGlobalConfig.builder()
            .setUserWorkerJobSettings(
                Windmill.UserWorkerRunnerV1Settings.newBuilder()
                    .setUseWindmillIsolatedChannels(true)
                    .build())
            .build());
    ManagedChannel channel =
        channelFactory.create(DEFAULT_FLOW_CONTROL_SETTINGS, DEFAULT_SERVICE_ADDRESS);
    assertThat(channel).isInstanceOf(IsolationChannel.class);
  }

  @Test
  public void testCreate_isolationChannelDisabled() {
    channelFactory.tryConsumeJobConfig(
        StreamingGlobalConfig.builder()
            .setUserWorkerJobSettings(
                Windmill.UserWorkerRunnerV1Settings.newBuilder()
                    .setUseWindmillIsolatedChannels(false)
                    .build())
            .build());
    ManagedChannel channel =
        channelFactory.create(DEFAULT_FLOW_CONTROL_SETTINGS, DEFAULT_SERVICE_ADDRESS);
    assertThat(channel).isNotInstanceOf(IsolationChannel.class);
  }

  @Test
  public void testCreate_afterNewJobConfig() {
    ManagedChannel initialChannel =
        channelFactory.create(DEFAULT_FLOW_CONTROL_SETTINGS, DEFAULT_SERVICE_ADDRESS);
    assertThat(initialChannel).isNotInstanceOf(IsolationChannel.class);

    // Consume a job config useWindmillIsolatedChannels = true.
    channelFactory.tryConsumeJobConfig(
        StreamingGlobalConfig.builder()
            .setUserWorkerJobSettings(
                Windmill.UserWorkerRunnerV1Settings.newBuilder()
                    .setUseWindmillIsolatedChannels(true)
                    .build())
            .build());
    ManagedChannel isolationChannel =
        channelFactory.create(DEFAULT_FLOW_CONTROL_SETTINGS, DEFAULT_SERVICE_ADDRESS);
    assertThat(isolationChannel).isInstanceOf(IsolationChannel.class);

    // Consume a job config useWindmillIsolatedChannels = false.
    channelFactory.tryConsumeJobConfig(
        StreamingGlobalConfig.builder()
            .setUserWorkerJobSettings(
                Windmill.UserWorkerRunnerV1Settings.newBuilder()
                    .setUseWindmillIsolatedChannels(false)
                    .build())
            .build());
    ManagedChannel notIsolationChannel =
        channelFactory.create(DEFAULT_FLOW_CONTROL_SETTINGS, DEFAULT_SERVICE_ADDRESS);
    assertThat(notIsolationChannel).isNotInstanceOf(IsolationChannel.class);
  }

  @Test
  public void testTryConsumeJobConfig_initialJobConfig() {
    assertTrue(
        channelFactory.tryConsumeJobConfig(
            StreamingGlobalConfig.builder()
                .setUserWorkerJobSettings(
                    Windmill.UserWorkerRunnerV1Settings.newBuilder()
                        .setUseWindmillIsolatedChannels(false)
                        .build())
                .build()));
  }

  @Test
  public void testTryConsumeJobConfig_sameIsolationChannelSetting() {
    assertTrue(
        channelFactory.tryConsumeJobConfig(
            StreamingGlobalConfig.builder()
                .setUserWorkerJobSettings(
                    Windmill.UserWorkerRunnerV1Settings.newBuilder()
                        .setUseWindmillIsolatedChannels(false)
                        .build())
                .build()));

    assertFalse(
        channelFactory.tryConsumeJobConfig(
            StreamingGlobalConfig.builder()
                .setUserWorkerJobSettings(
                    Windmill.UserWorkerRunnerV1Settings.newBuilder()
                        .setUseWindmillIsolatedChannels(false)
                        .build())
                .build()));
  }

  @Test
  public void testTryConsumeJobConfig_differentIsolationChannelSetting() {
    assertTrue(
        channelFactory.tryConsumeJobConfig(
            StreamingGlobalConfig.builder()
                .setUserWorkerJobSettings(
                    Windmill.UserWorkerRunnerV1Settings.newBuilder()
                        .setUseWindmillIsolatedChannels(false)
                        .build())
                .build()));

    assertTrue(
        channelFactory.tryConsumeJobConfig(
            StreamingGlobalConfig.builder()
                .setUserWorkerJobSettings(
                    Windmill.UserWorkerRunnerV1Settings.newBuilder()
                        .setUseWindmillIsolatedChannels(true)
                        .build())
                .build()));
  }
}
