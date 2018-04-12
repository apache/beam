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
package org.apache.beam.sdk.io.kafka;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.common.utils.Utils;

/**
 * This is a copy of Kafka's {@link org.apache.kafka.common.header.internals.RecordHeader}. Included
 * here in order to support older Kafka versions (0.9.x).
 */
public class KafkaRecordHeader implements KafkaHeader {

  private final String key;
  private ByteBuffer valueBuffer;
  private byte[] value;

  public KafkaRecordHeader(String key, byte[] value) {
    Objects.requireNonNull(key, "Null header keys are not permitted");
    this.key = key;
    this.value = value;
  }

  public KafkaRecordHeader(String key, ByteBuffer valueBuffer) {
    Objects.requireNonNull(key, "Null header keys are not permitted");
    this.key = key;
    this.valueBuffer = valueBuffer;
  }

  public String key() {
    return key;
  }

  public byte[] value() {
    if (value == null && valueBuffer != null) {
      value = Utils.toArray(valueBuffer);
      valueBuffer = null;
    }
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaRecordHeader header = (KafkaRecordHeader) o;
    return (key == null ? header.key == null : key.equals(header.key))
        && Arrays.equals(value(), header.value());
  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + Arrays.hashCode(value());
    return result;
  }

  @Override
  public String toString() {
    return "RecordHeader(key = " + key + ", value = " + Arrays.toString(value()) + ")";
  }
}
