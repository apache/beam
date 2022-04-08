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

import com.google.auto.value.AutoValue;
import java.io.Serializable;

@AutoValue
public abstract class SqsMessage implements Serializable {

  /** Message body. */
  public abstract String getBody();

  /** SQS message id. */
  public abstract String getMessageId();

  /** SQS receipt handle. */
  public abstract String getReceiptHandle();

  /** Timestamp the message was sent at (in epoch millis). */
  public abstract long getTimeStamp();

  /** Timestamp the message was received at (in epoch millis). */
  public abstract long getRequestTimeStamp();

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setBody(String body);

    abstract Builder setMessageId(String messageId);

    abstract Builder setReceiptHandle(String receiptHandle);

    abstract Builder setTimeStamp(long timeStamp);

    abstract Builder setRequestTimeStamp(long timeStamp);

    abstract SqsMessage build();
  }

  public static SqsMessage create(
      String body, String messageId, String receiptHandle, long timeStamp, long requestTimeStamp) {
    return new AutoValue_SqsMessage.Builder()
        .setBody(body)
        .setMessageId(messageId)
        .setReceiptHandle(receiptHandle)
        .setTimeStamp(timeStamp)
        .setRequestTimeStamp(requestTimeStamp)
        .build();
  }
}
