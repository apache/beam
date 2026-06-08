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

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

/**
 * The TextMessageMapper takes a {@link String} value, a {@link jakarta.jms.Session} and returns a
 * {@link jakarta.jms.TextMessage}.
 */
public class TextMessageMapper implements SerializableBiFunction<String, Session, Message> {

  @Override
  public Message apply(String value, Session session) {
    try {
      TextMessage msg = session.createTextMessage();
      msg.setText(value);
      return msg;
    } catch (JMSException e) {
      throw new JmsIOException("Error creating TextMessage", e);
    }
  }
}
