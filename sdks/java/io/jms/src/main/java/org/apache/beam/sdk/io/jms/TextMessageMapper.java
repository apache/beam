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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

/**
 * The TextMessageMapper takes a {@link String} value, a {@link javax.jms.Session} and returns a
 * {@link javax.jms.TextMessage}.
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
