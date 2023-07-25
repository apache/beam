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
package org.apache.beam.sdk.io.pulsar;

/**
 * Class representing a Pulsar Message record. Each PulsarMessage contains a single message basic
 * message data and Message record to access directly.
 */
@SuppressWarnings("initialization.fields.uninitialized")
public class PulsarMessage {
  private String topic;
  private Long publishTimestamp;
  private Object messageRecord;

  public PulsarMessage(String topic, Long publishTimestamp, Object messageRecord) {
    this.topic = topic;
    this.publishTimestamp = publishTimestamp;
    this.messageRecord = messageRecord;
  }

  public PulsarMessage(String topic, Long publishTimestamp) {
    this.topic = topic;
    this.publishTimestamp = publishTimestamp;
  }

  public String getTopic() {
    return topic;
  }

  public Long getPublishTimestamp() {
    return publishTimestamp;
  }

  public void setMessageRecord(Object messageRecord) {
    this.messageRecord = messageRecord;
  }

  public Object getMessageRecord() {
    return messageRecord;
  }
}
