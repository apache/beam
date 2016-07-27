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

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.UnboundedSource;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Message;

/**
 * Checkpoint for an unbounded JmsIO.Read. Consists of
 * JMS destination name, and the latest message ID consumed so far.
 */
@DefaultCoder(AvroCoder.class)
public class JmsCheckpointMark implements UnboundedSource.CheckpointMark {

  private final List<Message> messages = new ArrayList<>();

  public JmsCheckpointMark() {
  }

  public List<Message> getMessages() {
    return this.messages;
  }

  public void addMessage(Message message) {
    messages.add(message);
  }

  @Override
  public void finalizeCheckpoint() {
    for (Message message : messages) {
      try {
        message.acknowledge();
      } catch (Exception e) {
        // nothing to do
      }
    }
    messages.clear();
  }

}
