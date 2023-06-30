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
package org.apache.beam.sdk.io.jms.utils;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.Random;
import javax.jms.*;
import org.apache.beam.sdk.io.jms.JmsIOException;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

public class JmsIOTestUtils {

  private JmsIOTestUtils() {}

  public static class TestEvent implements Serializable {
    private final String topicName;
    private final String value;

    public TestEvent(String topicName, String value) {
      this.topicName = topicName;
      this.value = value;
    }

    public String getTopicName() {
      return this.topicName;
    }

    public String getValue() {
      return this.value;
    }
  }

  public static class TextMessageMapperWithError
      implements SerializableBiFunction<String, Session, Message> {
    @Override
    public Message apply(String value, Session session) {
      try {
        if (value.equals("Message 1") || value.equals("Message 2")) {
          throw new JMSException("Error!!");
        }
        TextMessage msg = session.createTextMessage();
        msg.setText(value);
        return msg;
      } catch (JMSException e) {
        throw new JmsIOException("Error creating TextMessage", e);
      }
    }
  }

  public static class TextMessageMapperWithErrorCounter
      implements SerializableBiFunction<String, Session, Message> {

    private static int errorCounter = 0;

    @Override
    public Message apply(String value, Session session) {
      try {
        if (errorCounter == 0) {
          errorCounter++;
          throw new JMSException("Error!!");
        }
        TextMessage msg = session.createTextMessage();
        msg.setText(value);
        return msg;
      } catch (JMSException e) {
        throw new JmsIOException("Error creating TextMessage", e);
      }
    }
  }

  public static class TextMessageMapperWithErrorAndCounter
      implements SerializableBiFunction<String, Session, Message> {
    private static int errorCounter = 0;

    @Override
    public Message apply(String value, Session session) {
      try {
        if (value.equals("Message 1") || value.equals("Message 2")) {
          if (errorCounter != 0 && value.equals("Message 1")) {
            TextMessage msg = session.createTextMessage();
            msg.setText(value);
            return msg;
          }
          errorCounter++;
          throw new JMSException("Error!!");
        }
        TextMessage msg = session.createTextMessage();
        msg.setText(value);
        return msg;
      } catch (JMSException e) {
        throw new JmsIOException("Error creating TextMessage", e);
      }
    }
  }

  public static class TextMessageMapperWithSessionClosed
      implements SerializableBiFunction<String, Session, Message> {
    private static int errorCounter = 0;

    @Override
    public Message apply(String value, Session session) {
      try {
        if (value.equals("Message 1") || value.equals("Message 2")) {
          if (errorCounter == 0) {
            errorCounter++;
            session.close();
          }
        }
        TextMessage msg = session.createTextMessage();
        msg.setText(value);
        return msg;
      } catch (JMSException e) {
        throw new JmsIOException("Error creating TextMessage", e);
      }
    }
  }

  private static int getRandomNumber() {
    int max = 65535, min = 1024;
    return new Random().nextInt(max - min + 1) + min;
  }

  private static boolean isPortUnused(int portNumber) {
    try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
      serverSocket.close();
      return true;
    } catch (IOException exception) {
      return false;
    }
  }

  public static int getUnusedPort() {
    int portNumber = getRandomNumber();
    while (true) {
      if (isPortUnused(portNumber)) {
        return portNumber;
      } else {
        portNumber = getRandomNumber();
      }
    }
  }
}
