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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerRunnerV1Settings;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactoryFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class GrpcDispatcherClientTest {

  static class GrpcDispatcherClientTestBase {

    @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    final ChannelCachingStubFactory stubFactory =
        new FakeWindmillStubFactory(
            () ->
                grpcCleanup.register(
                    WindmillChannelFactory.inProcessChannel("GrpcDispatcherClientTestChannel")));

    static StreamingGlobalConfig getGlobalConfig(boolean useWindmillIsolatedChannels) {
      return StreamingGlobalConfig.builder()
          .setWindmillServiceEndpoints(ImmutableSet.of(HostAndPort.fromString("windmill:1234")))
          .setUserWorkerJobSettings(
              UserWorkerRunnerV1Settings.newBuilder()
                  .setUseWindmillIsolatedChannels(useWindmillIsolatedChannels)
                  .build())
          .build();
    }
  }

  @RunWith(JUnit4.class)
  public static class RespectsJobSettingTest extends GrpcDispatcherClientTestBase {

    @Test
    public void createsNewStubWhenIsolatedChannelsConfigIsChanged() {
      DataflowWorkerHarnessOptions options =
          PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
      options.setExperiments(
          Lists.newArrayList(
              GrpcDispatcherClient.STREAMING_ENGINE_USE_JOB_SETTINGS_FOR_ISOLATED_CHANNELS));
      GrpcDispatcherClient dispatcherClient =
          GrpcDispatcherClient.create(options, new FakeWindmillStubFactoryFactory(stubFactory));
      // Create first time with Isolated channels disabled
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ false));
      CloudWindmillServiceV1Alpha1Stub stub1 = dispatcherClient.getWindmillServiceStub();
      CloudWindmillServiceV1Alpha1Stub stub2 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub2, stub1);

      // Enable Isolated channels
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ true));
      CloudWindmillServiceV1Alpha1Stub stub3 = dispatcherClient.getWindmillServiceStub();
      assertNotSame(stub3, stub1);

      CloudWindmillServiceV1Alpha1Stub stub4 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub3, stub4);

      // Disable Isolated channels
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ false));
      CloudWindmillServiceV1Alpha1Stub stub5 = dispatcherClient.getWindmillServiceStub();
      assertNotSame(stub4, stub5);
    }
  }

  @RunWith(Parameterized.class)
  public static class RespectsPipelineOptionsTest extends GrpcDispatcherClientTestBase {

    @Parameters
    public static Collection<Object[]> data() {
      List<Object[]> list = new ArrayList<>();
      for (Boolean pipelineOption : new Boolean[] {null, true, false}) {
        list.add(new Object[] {/*experimentEnabled=*/ false, pipelineOption});
      }
      for (Boolean pipelineOption : new Boolean[] {true, false}) {
        list.add(new Object[] {/*experimentEnabled=*/ true, pipelineOption});
      }
      return list;
    }

    @Parameter(0)
    public Boolean experimentEnabled;

    @Parameter(1)
    public Boolean pipelineOption;

    @Test
    public void ignoresIsolatedChannelsConfigWithPipelineOption() {
      DataflowWorkerHarnessOptions options =
          PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
      if (experimentEnabled) {
        options.setExperiments(
            Lists.newArrayList(
                GrpcDispatcherClient.STREAMING_ENGINE_USE_JOB_SETTINGS_FOR_ISOLATED_CHANNELS));
      }
      options.setUseWindmillIsolatedChannels(pipelineOption);
      GrpcDispatcherClient dispatcherClient =
          GrpcDispatcherClient.create(options, new FakeWindmillStubFactoryFactory(stubFactory));

      // Job setting disabled, PipelineOption enabled
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ false));
      CloudWindmillServiceV1Alpha1Stub stub1 = dispatcherClient.getWindmillServiceStub();
      CloudWindmillServiceV1Alpha1Stub stub2 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub2, stub1);

      // Job setting enabled
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ true));
      CloudWindmillServiceV1Alpha1Stub stub3 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub3, stub1);

      CloudWindmillServiceV1Alpha1Stub stub4 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub3, stub4);

      // Job setting disabled
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ false));
      CloudWindmillServiceV1Alpha1Stub stub5 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub4, stub5);
    }
  }
}
