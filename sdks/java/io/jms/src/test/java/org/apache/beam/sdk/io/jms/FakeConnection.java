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
package org.apache.beam.sdk.io.jms;

import java.io.Serializable;
import javax.jms.*;
import org.apache.activemq.AlreadyClosedException;
import org.apache.activemq.ConnectionFailedException;
import org.mockito.Mockito;

public class FakeConnection implements Connection, Serializable {

  private ExceptionListener listener;
  private static int COUNTER = 0;

  @Override
  public Session createSession(boolean transacted, int acknowledgeMode) {
    return Mockito.mock(Session.class);
  }

  @Override
  public String getClientID() {
    return null;
  }

  @Override
  public void setClientID(String clientID) {}

  @Override
  public ConnectionMetaData getMetaData() {
    return null;
  }

  @Override
  public ExceptionListener getExceptionListener() {
    return listener;
  }

  @Override
  public void setExceptionListener(ExceptionListener listener) {
    this.listener = listener;
  }

  @Override
  public void start() {
    if (COUNTER == 0) {
      COUNTER++;
      this.listener.onException(new ConnectionFailedException());
    } else {
      this.listener.onException(new AlreadyClosedException());
    }
  }

  @Override
  public void stop() {}

  @Override
  public void close() {}

  @Override
  public ConnectionConsumer createConnectionConsumer(
      Destination destination,
      String messageSelector,
      ServerSessionPool sessionPool,
      int maxMessages) {
    return null;
  }

  @Override
  public ConnectionConsumer createDurableConnectionConsumer(
      Topic topic,
      String subscriptionName,
      String messageSelector,
      ServerSessionPool sessionPool,
      int maxMessages) {
    return null;
  }
}
