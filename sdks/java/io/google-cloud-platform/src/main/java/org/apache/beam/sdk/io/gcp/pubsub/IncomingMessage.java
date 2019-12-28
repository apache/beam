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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.auto.value.AutoValue;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.beam.sdk.coders.DefaultCoder;

/** A message received from Pubsub. */
@AutoValue
@DefaultCoder(IncomingMessageCoder.class)
abstract class IncomingMessage {

  /** Underlying Message. */
  public abstract com.google.pubsub.v1.PubsubMessage message();

  /**
   * Timestamp for element (ms since epoch). Either Pubsub's processing time, or the custom
   * timestamp associated with the message.
   */
  public abstract long timestampMsSinceEpoch();

  /** Timestamp (in system time) at which we requested the message (ms since epoch). */
  public abstract long requestTimeMsSinceEpoch();

  /** Id to pass back to Pubsub to acknowledge receipt of this message. */
  public abstract String ackId();

  /** Id to pass to the runner to distinguish this message from all others. */
  public abstract String recordId();

  public static IncomingMessage of(
      PubsubMessage message,
      long timestampMsSinceEpoch,
      long requestTimeMsSinceEpoch,
      String ackId,
      String recordId) {
    return new AutoValue_IncomingMessage(
        message, timestampMsSinceEpoch, requestTimeMsSinceEpoch, ackId, recordId);
  }
}
