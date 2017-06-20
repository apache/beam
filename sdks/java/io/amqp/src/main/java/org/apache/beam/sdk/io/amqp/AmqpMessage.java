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
package org.apache.beam.sdk.io.amqp;

import org.apache.qpid.proton.message.Message;

/**
 * Extend AMQ ProtonJ Message to override the equals() method.
 */
public class AmqpMessage {

  private Message message;

  public AmqpMessage() {
    message = Message.Factory.create();
  }

  public AmqpMessage(Message message) {
    this.message = message;
  }

  public Message getMessage() {
    return this.message;
  }

  @Override
  public int hashCode() {
    return this.message.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AmqpMessage) {
      AmqpMessage message = (AmqpMessage) obj;
      if (this.message.getBody().toString().equals(message.getMessage().getBody().toString())) {
        return true;
      }
    }
    return false;
  }

}
