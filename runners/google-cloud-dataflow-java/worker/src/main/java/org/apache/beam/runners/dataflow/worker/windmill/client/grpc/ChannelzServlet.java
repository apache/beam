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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.options.DataflowStreamingPipelineOptions;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.channelz.v1.*;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.protobuf.services.ChannelzService;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;

/** Respond to /path with the GRPC channelz data. */
@Internal
public class ChannelzServlet extends BaseStatusServlet implements DebugCapture.Capturable {

  private static final int MAX_TOP_CHANNELS_TO_RETURN = 500;
  private static final String CHANNELZ_PATH = "/channelz";

  private final ChannelzService channelzService;
  private final Supplier<ImmutableSet<HostAndPort>> currentWindmillEndpoints;
  private final boolean showOnlyWindmillServiceChannels;

  public ChannelzServlet(
      DataflowStreamingPipelineOptions options,
      Supplier<ImmutableSet<HostAndPort>> currentWindmillEndpoints) {
    super(CHANNELZ_PATH);
    channelzService = ChannelzService.newInstance(MAX_TOP_CHANNELS_TO_RETURN);
    this.currentWindmillEndpoints = currentWindmillEndpoints;
    showOnlyWindmillServiceChannels = options.getChannelzShowOnlyWindmillServiceChannels();
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    response.setStatus(HttpServletResponse.SC_OK);
    PrintWriter writer = response.getWriter();
    captureData(writer);
  }

  @Override
  public String pageName() {
    return getPath();
  }

  @Override
  public void captureData(PrintWriter writer) {
    writer.println("<html>");
    writer.println("<h1>Channelz</h1>");
    appendTopChannels(writer);
    writer.println("</html>");
  }

  private void appendTopChannels(PrintWriter writer) {
    SettableFuture<GetTopChannelsResponse> future = SettableFuture.create();
    // IDEA: If there are more than MAX_TOP_CHANNELS_TO_RETURN top channels
    // in the worker, we might not return all the windmill channels. If we run into
    // such situations, this logic can be modified to loop till we see an empty
    // GetTopChannelsResponse response with the end bit set.
    channelzService.getTopChannels(
        GetTopChannelsRequest.newBuilder().build(), getStreamObserver(future));
    GetTopChannelsResponse topChannelsResponse;
    try {
      topChannelsResponse = future.get();
    } catch (Exception e) {
      String msg = "Failed to get channelz: " + e.getMessage();
      writer.println(msg);
      return;
    }

    List<Channel> topChannels = topChannelsResponse.getChannelList();
    if (showOnlyWindmillServiceChannels) {
      topChannels = filterWindmillChannels(topChannels);
    }
    writer.println("<h2>Top Level Channels</h2>");
    writer.println("<table border='1'>");
    VisitedSets visitedSets = new VisitedSets();
    for (Channel channel : topChannels) {
      writer.println("<tr>");
      writer.println("<td>");
      writer.println("TopChannelId: " + channel.getRef().getChannelId());
      writer.println("</td>");
      writer.println("<td>");
      appendChannel(channel, writer, visitedSets);
      writer.println("</td>");
      writer.println("</tr>");
    }
    writer.println("</table>");
  }

  private List<Channel> filterWindmillChannels(List<Channel> channels) {
    ImmutableSet<HostAndPort> windmillServiceEndpoints = currentWindmillEndpoints.get();
    Set<String> windmillServiceHosts =
        windmillServiceEndpoints.stream().map(HostAndPort::getHost).collect(Collectors.toSet());
    List<Channel> windmillChannels = new ArrayList<>();
    for (Channel channel : channels) {
      for (String windmillServiceHost : windmillServiceHosts) {
        if (channel.getData().getTarget().contains(windmillServiceHost)) {
          windmillChannels.add(channel);
          break;
        }
      }
    }
    return windmillChannels;
  }

  private void appendChannels(
      List<ChannelRef> channelRefs, PrintWriter writer, VisitedSets visitedSets) {
    for (ChannelRef channelRef : channelRefs) {
      writer.println("<tr>");
      writer.println("<td>");
      writer.println("Channel: " + channelRef.getChannelId());
      writer.println("</td>");
      writer.println("<td>");
      appendChannel(channelRef, writer, visitedSets);
      writer.println("</td>");
      writer.println("</tr>");
    }
  }

  private void appendChannel(ChannelRef channelRef, PrintWriter writer, VisitedSets visitedSets) {
    if (visitedSets.channels.contains(channelRef.getChannelId())) {
      String msg = "Duplicate Channel Id: " + channelRef;
      writer.println(msg);
      return;
    }
    visitedSets.channels.add(channelRef.getChannelId());
    SettableFuture<GetChannelResponse> future = SettableFuture.create();
    channelzService.getChannel(
        GetChannelRequest.newBuilder().setChannelId(channelRef.getChannelId()).build(),
        getStreamObserver(future));
    Channel channel;
    try {
      channel = future.get().getChannel();
    } catch (Exception e) {
      String msg = "Failed to get Channel: " + channelRef;
      writer.println(msg + " Exception: " + e.getMessage());
      return;
    }
    appendChannel(channel, writer, visitedSets);
  }

  private void appendChannel(Channel channel, PrintWriter writer, VisitedSets visitedSets) {
    writer.println("<table border='1'>");
    writer.println("<tr>");
    writer.println("<td>");
    writer.println("ChannelId: " + channel.getRef().getChannelId());
    writer.println("</td>");
    writer.println("<td><pre>" + channel);
    writer.println("</pre></td>");
    writer.println("</tr>");
    appendChannels(channel.getChannelRefList(), writer, visitedSets);
    appendSubChannels(channel.getSubchannelRefList(), writer, visitedSets);
    appendSockets(channel.getSocketRefList(), writer);
    writer.println("</table>");
  }

  private void appendSubChannels(
      List<SubchannelRef> subchannelRefList, PrintWriter writer, VisitedSets visitedSets) {
    for (SubchannelRef subchannelRef : subchannelRefList) {
      writer.println("<tr>");
      writer.println("<td>");
      writer.println("Sub Channel: " + subchannelRef.getSubchannelId());
      writer.println("</td>");
      writer.println("<td>");
      appendSubchannel(subchannelRef, writer, visitedSets);
      writer.println("</td>");
      writer.println("</tr>");
    }
  }

  private void appendSubchannel(
      SubchannelRef subchannelRef, PrintWriter writer, VisitedSets visitedSets) {
    if (visitedSets.subchannels.contains(subchannelRef.getSubchannelId())) {
      String msg = "Duplicate Subchannel Id: " + subchannelRef;
      writer.println(msg);
      return;
    }
    visitedSets.subchannels.add(subchannelRef.getSubchannelId());
    SettableFuture<GetSubchannelResponse> future = SettableFuture.create();
    channelzService.getSubchannel(
        GetSubchannelRequest.newBuilder().setSubchannelId(subchannelRef.getSubchannelId()).build(),
        getStreamObserver(future));
    Subchannel subchannel;
    try {
      subchannel = future.get().getSubchannel();
    } catch (Exception e) {
      String msg = "Failed to get Subchannel: " + subchannelRef;
      writer.println(msg + " Exception: " + e.getMessage());
      return;
    }

    writer.println("<table border='1'>");
    writer.println("<tr>");
    writer.println("<td>SubchannelId: " + subchannelRef.getSubchannelId());
    writer.println("</td>");
    writer.println("<td><pre>" + subchannel.toString());
    writer.println("</pre></td>");
    writer.println("</tr>");
    appendChannels(subchannel.getChannelRefList(), writer, visitedSets);
    appendSubChannels(subchannel.getSubchannelRefList(), writer, visitedSets);
    appendSockets(subchannel.getSocketRefList(), writer);
    writer.println("</table>");
  }

  private void appendSockets(List<SocketRef> socketRefList, PrintWriter writer) {
    for (SocketRef socketRef : socketRefList) {
      writer.println("<tr>");
      writer.println("<td>");
      writer.println("Socket: " + socketRef.getSocketId());
      writer.println("</td>");
      writer.println("<td>");
      appendSocket(socketRef, writer);
      writer.println("</td>");
      writer.println("</tr>");
    }
  }

  private void appendSocket(SocketRef socketRef, PrintWriter writer) {
    SettableFuture<GetSocketResponse> future = SettableFuture.create();
    channelzService.getSocket(
        GetSocketRequest.newBuilder().setSocketId(socketRef.getSocketId()).build(),
        getStreamObserver(future));
    Socket socket;
    try {
      socket = future.get().getSocket();
    } catch (Exception e) {
      String msg = "Failed to get Socket: " + socketRef;
      writer.println(msg + " Exception: " + e.getMessage());
      return;
    }
    writer.println("<pre>" + socket + "</pre>");
  }

  private <T> StreamObserver<T> getStreamObserver(SettableFuture<T> future) {
    return new StreamObserver<T>() {
      @Nullable T response = null;

      @Override
      public void onNext(T message) {
        response = message;
      }

      @Override
      public void onError(Throwable throwable) {
        future.setException(throwable);
      }

      @Override
      public void onCompleted() {
        future.set(response);
      }
    };
  }

  // channelz proto says there won't be cycles in the ref graph.
  // we track visited ids to be defensive and prevent any accidental cycles.
  private static class VisitedSets {

    Set<Long> channels = new HashSet<>();
    Set<Long> subchannels = new HashSet<>();
  }
}
