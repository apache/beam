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
package org.apache.beam.sdk.io.mqtt;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.joda.time.Instant;

/**
 * Checkpoint for an unbounded {@link org.apache.beam.sdk.io.mqtt.MqttIO.Read}.
 * It consists in a list of pending message (not possible to control the ACK in MQTT, all depends
 * of the QoS) and the timestamp of the oldest message.
 */
@DefaultCoder(SerializableCoder.class)
public class MqttCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  private final List<MqttMessage> messages = new ArrayList<>();
  private Instant oldestTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public MqttCheckpointMark() {
  }

  public Instant addMessage(MqttMessage message) {
    if (messages.size() < 1) {
      oldestTimestamp = Instant.now();
    }
    messages.add(message);
    return oldestTimestamp;
  }

  public Instant getOldestTimestamp() {
    return oldestTimestamp;
  }

  @Override
  public void finalizeCheckpoint() {
    messages.clear();
  }

}
