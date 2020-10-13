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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import com.google.auto.value.AutoValue;
import com.google.cloud.ByteArray;
import java.io.Serializable;

/** This class is created because Kafka Consumer Records are not serializable. */
@AutoValue
public abstract class KafkaTestRecord implements Serializable {

  public abstract String getKey();

  public abstract ByteArray getValue();

  public abstract String getTopic();

  public abstract long getTimeStamp();

  public static KafkaTestRecord create(
      String newKey, byte[] newValue, String newTopic, long newTimeStamp) {
    return new AutoValue_KafkaTestRecord(
        newKey, ByteArray.copyFrom(newValue), newTopic, newTimeStamp);
  }
}
