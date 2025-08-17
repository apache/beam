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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerRunnerV1Settings;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.IsolationChannel;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactoryFactoryImpl;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class GrpcDispatcherClientTest {

  @RunWith(JUnit4.class)
  public static class RespectsJobSettingTest {

    @Test
    public void createsNewStubWhenIsolatedChannelsConfigIsChanged() {
      DataflowWorkerHarnessOptions options =
          PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
      GrpcDispatcherClient dispatcherClient =
          GrpcDispatcherClient.create(options, new WindmillStubFactoryFactoryImpl(options));
      // Create first time with Isolated channels disabled
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ false));
      CloudWindmillServiceV1Alpha1Stub stub1 = dispatcherClient.getWindmillServiceStub();
      CloudWindmillServiceV1Alpha1Stub stub2 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub2, stub1);
      assertThat(stub1.getChannel(), not(instanceOf(IsolationChannel.class)));

      // Enable Isolated channels
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ true));
      CloudWindmillServiceV1Alpha1Stub stub3 = dispatcherClient.getWindmillServiceStub();
      assertNotSame(stub3, stub1);

      assertThat(stub3.getChannel(), instanceOf(IsolationChannel.class));
      CloudWindmillServiceV1Alpha1Stub stub4 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub3, stub4);

      // Disable Isolated channels
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ false));
      CloudWindmillServiceV1Alpha1Stub stub5 = dispatcherClient.getWindmillServiceStub();
      assertNotSame(stub4, stub5);
      assertThat(stub5.getChannel(), not(instanceOf(IsolationChannel.class)));
    }
  }

  @RunWith(Parameterized.class)
  public static class RespectsPipelineOptionsTest {

    @Parameters
    public static Collection<Object[]> data() {
      List<Object[]> list = new ArrayList<>();
      for (Boolean pipelineOption : new Boolean[] {true, false}) {
        list.add(new Object[] {pipelineOption});
      }
      return list;
    }

    @Parameter(0)
    public Boolean pipelineOption;

    @Test
    public void ignoresIsolatedChannelsConfigWithPipelineOption() {
      DataflowWorkerHarnessOptions options =
          PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
      options.setUseWindmillIsolatedChannels(pipelineOption);
      GrpcDispatcherClient dispatcherClient =
          GrpcDispatcherClient.create(options, new WindmillStubFactoryFactoryImpl(options));
      Matcher<Object> classMatcher =
          pipelineOption
              ? instanceOf(IsolationChannel.class)
              : not(instanceOf(IsolationChannel.class));

      // Job setting disabled, PipelineOption enabled
      dispatcherClient.onJobConfig(getGlobalConfig(/*useWindmillIsolatedChannels=*/ false));
      CloudWindmillServiceV1Alpha1Stub stub1 = dispatcherClient.getWindmillServiceStub();
      CloudWindmillServiceV1Alpha1Stub stub2 = dispatcherClient.getWindmillServiceStub();
      assertSame(stub2, stub1);
      assertThat(stub1.getChannel(), classMatcher);

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
