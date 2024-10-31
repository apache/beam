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

import static org.junit.Assert.*;

import java.io.*;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.FakeWindmillServer;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChannelzServletTest {

  @Test
  public void testRendersAllChannels() throws UnsupportedEncodingException {
    String windmill1 = "WindmillHost1";
    String windmill2 = "WindmillHost2";
    String nonWindmill1 = "NonWindmillHost1";
    String someOtherHost1 = "SomeOtherHost2";
    ManagedChannel[] unusedChannels =
        new ManagedChannel[] {
          InProcessChannelBuilder.forName(windmill1).build(),
          InProcessChannelBuilder.forName(windmill2).build(),
          InProcessChannelBuilder.forName(nonWindmill1).build(),
          InProcessChannelBuilder.forName(someOtherHost1).build()
        };
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.create().as(DataflowWorkerHarnessOptions.class);
    FakeWindmillServer fakeWindmillServer =
        new FakeWindmillServer(new ErrorCollector(), s -> Optional.empty());
    fakeWindmillServer.setWindmillServiceEndpoints(
        ImmutableSet.of(HostAndPort.fromHost(windmill1), HostAndPort.fromHost(windmill2)));
    options.setChannelzShowOnlyWindmillServiceChannels(false);
    ChannelzServlet channelzServlet =
        new ChannelzServlet(options, fakeWindmillServer::getWindmillServiceEndpoints);
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);
    channelzServlet.captureData(writer);
    writer.flush();
    String channelzData = stringWriter.toString();
    assertTrue(channelzData.contains(windmill1));
    assertTrue(channelzData.contains(windmill2));
    assertTrue(channelzData.contains(nonWindmill1));
    assertTrue(channelzData.contains(someOtherHost1));
  }

  @Test
  public void testRendersOnlyWindmillChannels() throws UnsupportedEncodingException {
    String windmill1 = "WindmillHost1";
    String windmill2 = "WindmillHost2";
    String nonWindmill1 = "NonWindmillHost1";
    String someOtherHost1 = "SomeOtherHost2";
    ManagedChannel[] unusedChannels =
        new ManagedChannel[] {
          InProcessChannelBuilder.forName(windmill1).build(),
          InProcessChannelBuilder.forName(windmill2).build(),
          InProcessChannelBuilder.forName(nonWindmill1).build(),
          InProcessChannelBuilder.forName(someOtherHost1).build()
        };
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.create().as(DataflowWorkerHarnessOptions.class);
    FakeWindmillServer fakeWindmillServer =
        new FakeWindmillServer(new ErrorCollector(), s -> Optional.empty());
    fakeWindmillServer.setWindmillServiceEndpoints(
        ImmutableSet.of(HostAndPort.fromHost(windmill1), HostAndPort.fromHost(windmill2)));
    options.setChannelzShowOnlyWindmillServiceChannels(true);
    ChannelzServlet channelzServlet =
        new ChannelzServlet(options, fakeWindmillServer::getWindmillServiceEndpoints);
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);
    channelzServlet.captureData(writer);
    writer.flush();
    String channelzData = stringWriter.toString();
    assertTrue(channelzData.contains(windmill1));
    assertTrue(channelzData.contains(windmill2));
    // The logic does a substring match on the target
    // NonWindmillHost1 matches since it contains WindmillHost1 which is a windmill host
    assertTrue(channelzData.contains(nonWindmill1));
    assertFalse(channelzData.contains(someOtherHost1));
  }
}
