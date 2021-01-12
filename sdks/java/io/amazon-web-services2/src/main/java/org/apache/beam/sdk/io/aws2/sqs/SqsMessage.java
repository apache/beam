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
package org.apache.beam.sdk.io.aws2.sqs;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class SqsMessage implements Serializable {

  abstract @Nullable String getBody();

  abstract @Nullable String getMessageId();

  abstract @Nullable String getTimeStamp();

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setBody(String body);

    abstract Builder setMessageId(String messageId);

    abstract Builder setTimeStamp(String timeStamp);

    abstract SqsMessage build();
  }

  static SqsMessage create(String body, String messageId, String timeStamp) {
    checkArgument(body != null, "body can not be null");
    checkArgument(messageId != null, "messageId can not be null");
    checkArgument(timeStamp != null, "timeStamp can not be null");

    return new AutoValue_SqsMessage.Builder()
        .setBody(body)
        .setMessageId(messageId)
        .setTimeStamp(timeStamp)
        .build();
  }
}
