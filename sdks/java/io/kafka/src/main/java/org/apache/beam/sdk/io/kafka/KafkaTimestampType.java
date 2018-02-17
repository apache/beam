/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF E 2.0 (the
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

/**
 * This is a copy of Kafka's {@link org.apache.kafka.common.record.TimestampType}. Included
 * here in order to support older Kafka versions (0.9.x).
 */
public enum KafkaTimestampType {
  NO_TIMESTAMP_TYPE(-1, "NoTimestampType"),
  CREATE_TIME(0, "CreateTime"),
  LOG_APPEND_TIME(1, "LogAppendTime");

  public final int id;
  public final String name;

  KafkaTimestampType(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public static KafkaTimestampType forOrdinal(int ordinal) {
    return values()[ordinal];
  }

  @Override
  public String toString() {
    return name;
  }
}
