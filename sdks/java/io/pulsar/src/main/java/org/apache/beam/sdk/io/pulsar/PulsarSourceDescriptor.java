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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.pulsar.client.api.MessageId;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PulsarSourceDescriptor implements Serializable {

  @SchemaFieldName("topic")
  abstract String getTopic();

  @SchemaFieldName("start_offset")
  @Nullable
  abstract Long getStartOffset();

  @SchemaFieldName("end_offset")
  @Nullable
  abstract Long getEndOffset();

  @SchemaFieldName("end_messageid")
  @Nullable
  abstract MessageId getEndMessageId();

  @SchemaFieldName("client_url")
  abstract String getClientUrl();

  @SchemaFieldName("admin_url")
  abstract String getAdminUrl();

  public static PulsarSourceDescriptor of(
      String topic,
      Long startOffsetTimestamp,
      Long endOffsetTimestamp,
      MessageId endMessageId,
      String clientUrl,
      String adminUrl) {
    return new AutoValue_PulsarSourceDescriptor(
        topic, startOffsetTimestamp, endOffsetTimestamp, endMessageId, clientUrl, adminUrl);
  }
}
