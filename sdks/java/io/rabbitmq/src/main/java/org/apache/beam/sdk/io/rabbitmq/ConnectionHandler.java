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
import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

class ConnectionHandler implements ChannelLeaser, Closeable {
  private final Map<UUID, Channel> channelsByLessee = new ConcurrentHashMap<>();
  private final String uri;
  private Connection connection;

  public ConnectionHandler(String uri) {
    this.uri = uri;
  }

  @Override
  public Channel acquireChannel(UUID lesseeId) throws IOException {
    return getChannel(lesseeId);
  }

  @Override
  public void returnChannel(UUID lessee) {
    Channel toClose = channelsByLessee.remove(lessee);
    if (toClose != null) {
      try {
        toClose.close();
      } catch (IOException | TimeoutException e) {
        // ignore
      }
    }
  }

  public Channel getChannel(UUID lessee) throws IOException {
    if (connection == null) {
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

    if (connection != null) {
      connection.close();
    }
  }
}
