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

import java.util.Arrays;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A container class for MQTT message metadata, including the topic name and payload. */
public class MqttRecord {
  private final String topic;
  private final byte[] payload;

  public MqttRecord(String topic, byte[] payload) {
    this.topic = topic;
    this.payload = payload;
  }

  public String getTopic() {
    return topic;
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, Arrays.hashCode(payload));
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MqttRecord that = (MqttRecord) o;
    return Objects.equals(topic, that.topic) && Objects.deepEquals(payload, that.payload);
  }
}
