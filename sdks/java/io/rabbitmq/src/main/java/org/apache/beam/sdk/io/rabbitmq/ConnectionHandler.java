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
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class ConnectionHandler implements Closeable {

  private final String uri;
  private Connection connection;
  private Channel channel;

  public ConnectionHandler(String uri) {
    this.uri = uri;
  }

  public Channel getChannel() throws IOException {
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

    if (channel == null) {
      channel = connection.createChannel();
    }

    if (channel == null) {
      throw new IOException("No RabitMQ channel available");
    }
    return channel;
  }

  @Override
  public void close() throws IOException {
    if (channel != null) {
      try {
        channel.close();
      } catch (Exception e) {
        // ignore
      }
    }
    if (connection != null) {
      connection.close();
    }
  }
}
