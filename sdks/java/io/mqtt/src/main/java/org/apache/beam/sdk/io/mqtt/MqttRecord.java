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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/** A container class for MQTT message metadata, including the topic name and payload. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class MqttRecord {
  public abstract String getTopic();

  @SuppressWarnings("mutable")
  public abstract byte[] getPayload();

  static Builder builder() {
    return new AutoValue_MqttRecord.Builder();
  }

  static MqttRecord of(String topic, byte[] payload) {
    return builder().setTopic(topic).setPayload(payload).build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTopic(String topic);

    abstract Builder setPayload(byte[] payload);

    abstract MqttRecord build();
  }
}
