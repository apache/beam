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
package org.apache.beam.sdk.io.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MissedHeartbeatException;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.Closeable;
import java.io.IOException;
import java.net.ProtocolException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * RabbitMQ multiplexes over a single Connection using Channels so it should not be necessary to
 * open multiple Connections to a single host. This class implements {@link ChannelLeaser} by
 * maintaining a map of lessees and their associated Channels over a single Connection.
 *
 * <p>Note: it is unclear what, if anything, is ultimately responsible for calling close() here; it
 * should only happen on Beam runner shutdown, or if it were knowable that there were no current
 * Readers or Writers interacting with Rabbit.
 */
@ThreadSafe
class ConnectionHandler implements ChannelLeaser, Closeable {
  private final Map<UUID, Channel> channelsByLessee = new ConcurrentHashMap<>();
  private final String uri;
  private volatile Connection connection;

  public ConnectionHandler(String uri) {
    this.uri = uri;
  }

  @Override
  public <T> T useChannel(UUID lesseeId, ChannelLeaser.UseChannelFunction<T> f) throws IOException {
    try {
      Channel channel = getChannel(lesseeId);
      return f.apply(channel);
    } catch (PossibleAuthenticationFailureException
        | ProtocolException
        | MissedHeartbeatException e) {
      // full Connection-level problem
      close();
      throw e;
    } catch (ShutdownSignalException e) {
      // Connection- or Channel-level, depending
      Object cause = e.getReference();
      if (cause instanceof Channel) {
        closeChannel(lesseeId);
      }
      if (cause instanceof Connection) {
        close();
      }
      throw e;
    }
  }

  @Override
  public void closeChannel(UUID lessee) {
    Channel toClose = channelsByLessee.remove(lessee);
    if (toClose != null) {
      try {
        toClose.close();
      } catch (IOException | TimeoutException e) {
        // ignore
      }
    }
  }

  private Channel getChannel(UUID lessee) throws IOException {
    if (connection == null) {
      synchronized (this) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        try {
          connectionFactory.setUri(uri);
          connectionFactory.setAutomaticRecoveryEnabled(true);
          connectionFactory.setConnectionTimeout(60000);
          connectionFactory.setNetworkRecoveryInterval(5000);
          connectionFactory.setRequestedHeartbeat(60);
          connectionFactory.setTopologyRecoveryEnabled(true);
          connectionFactory.setRequestedChannelMax(0);
          connectionFactory.setRequestedFrameMax(0);
        } catch (URISyntaxException e) {
          // full URI excluded lest it contain user/pass
          throw new IOException("Unable to connect to rabbit; invalid URI", e);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
          throw new IOException("Security issue while connecting to rabbit: " + e.getMessage(), e);
        }

        try {
          connection = connectionFactory.newConnection();
        } catch (TimeoutException e) {
          throw new IOException("Timed out attempting to connect to rabbit", e);
        }
      }
    }

    return channelsByLessee.computeIfAbsent(
        lessee,
        (uuid) -> {
          try {
            return connection
                .openChannel()
                .orElseThrow(() -> new RuntimeException("No RabitMQ channel available"));
          } catch (IOException e) {
            throw new RuntimeException("No RabitMQ channel available");
          }
        });
  }

  /**
   * Closes all Channels, closes the connection, and clears the mapping of Channels by lessee.
   *
   * @throws IOException if an error occurs closing the underlying Connection. exceptions thrown
   *     while closing individual Channels are ignored
   */
  @Override
  public synchronized void close() throws IOException {
    channelsByLessee.forEach(
        (id, channel) -> {
          try {
            channel.close();
          } catch (Exception e) {
            /* ignore */
          }
        });
    channelsByLessee.clear();

    if (connection != null) {
      connection.close();
      connection = null;
    }
  }
}
