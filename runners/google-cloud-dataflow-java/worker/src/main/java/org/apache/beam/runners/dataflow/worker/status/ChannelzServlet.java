package org.apache.beam.runners.dataflow.worker.status;

import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.channelz.v1.*;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.protobuf.services.ChannelzService;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Respond to /channelz with the GRPC channelz data. */
@SuppressWarnings({
        "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ChannelzServlet extends BaseStatusServlet implements DebugCapture.Capturable {
    private static final String PATH = "/channelz";
    private static final Logger LOG = LoggerFactory.getLogger(ChannelzServlet.class);
    private static final int MAX_TOP_CHANNELS_TO_RETURN = 100;

    private final ChannelzService channelzService = ChannelzService.newInstance(MAX_TOP_CHANNELS_TO_RETURN);

    public ChannelzServlet() {
        super(PATH);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        response.setStatus(HttpServletResponse.SC_OK);
        PrintWriter writer = response.getWriter();
        captureData(writer);
    }

    @Override
    public String pageName() {
        return PATH;
    }

    @Override
    public void captureData(PrintWriter writer) {
        writer.println("<html>");
        writer.println("<h1>Channelz</h1>");
        appendTopChannels(writer);
        writer.println("</html>");
    }

    // channelz proto says there may not be cycles in the ref graph
    // we track visited ids to prevent any accidental cycles,
    static class VisitedSets {
        Set<Long> channels = new HashSet<>();
        Set<Long> subchannels = new HashSet<>();
    }

    private void appendTopChannels(PrintWriter writer) {
        SettableFuture<GetTopChannelsResponse> future = SettableFuture.create();
        channelzService.getTopChannels(
                GetTopChannelsRequest.newBuilder()
                        .build(), getStreamObserver(future));
        GetTopChannelsResponse topChannelsResponse;
        try {
            topChannelsResponse = future.get();
        } catch (Exception e) {
            String msg = "Failed to get channelz: " + e.getMessage();
            LOG.warn(msg, e);
            writer.println(msg);
            return;
        }
        writer.println("<h2>Top Level Channels</h2>");
        writer.println("<table border='1'>");
        VisitedSets visitedSets = new VisitedSets();
        for (Channel channel : topChannelsResponse.getChannelList()) {
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



    private void appendChannels(List<ChannelRef> channelRefs, PrintWriter writer, VisitedSets visitedSets) {
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
            LOG.warn(msg);
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
            LOG.warn(msg, e);
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

    private void appendSubChannels(List<SubchannelRef> subchannelRefList, PrintWriter writer, VisitedSets visitedSets) {
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

    private void appendSubchannel(SubchannelRef subchannelRef, PrintWriter writer, VisitedSets visitedSets) {
        if (visitedSets.subchannels.contains(subchannelRef.getSubchannelId())) {
            String msg = "Duplicate Subchannel Id: " + subchannelRef;
            LOG.warn(msg);
            writer.println(msg);
            return;
        }
        visitedSets.subchannels.add(subchannelRef.getSubchannelId());
        SettableFuture<GetSubchannelResponse> future = SettableFuture.create();
        channelzService.getSubchannel(
                GetSubchannelRequest.newBuilder().setSubchannelId(subchannelRef.getSubchannelId())
                        .build(),
                getStreamObserver(future));
        Subchannel subchannel;
        try {
            subchannel = future.get().getSubchannel();
        } catch (Exception e) {
            String msg = "Failed to get Subchannel: " + subchannelRef;
            LOG.warn(msg, e);
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
                GetSocketRequest.newBuilder().setSocketId(socketRef.getSocketId())
                        .build(),
                getStreamObserver(future));
        Socket socket;
        try {
            socket = future.get().getSocket();
        } catch (Exception e) {
            String msg = "Failed to get Socket: " + socketRef;
            LOG.warn(msg, e);
            writer.println(msg + " Exception: " + e.getMessage());
            return;
        }
        writer.println("<pre>" + socket + "</pre>");
    }

    private <T> StreamObserver<T> getStreamObserver(SettableFuture<T> future) {
        return new StreamObserver<T>() {
            T response = null;

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
}
