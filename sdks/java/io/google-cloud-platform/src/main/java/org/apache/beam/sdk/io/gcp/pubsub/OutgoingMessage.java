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
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;

/** A message to be sent to Pubsub. */
@AutoValue
@DefaultCoder(OutgoingMessageCoder.class)
abstract class OutgoingMessage {

  /** Underlying Message. May not have publish timestamp set. */
  public abstract com.google.pubsub.v1.PubsubMessage message();

  /** Timestamp for element (ms since epoch). */
  public abstract long timestampMsSinceEpoch();

  /**
   * If using an id attribute, the record id to associate with this record's metadata so the
   * receiver can reject duplicates. Otherwise {@literal null}.
   */
  @Nullable
  public abstract String recordId();

  public static OutgoingMessage of(
      com.google.pubsub.v1.PubsubMessage message,
      long timestampMsSinceEpoch,
      @Nullable String recordId) {
    return new AutoValue_OutgoingMessage(message, timestampMsSinceEpoch, recordId);
  }

  public static OutgoingMessage of(
      PubsubMessage message, long timestampMsSinceEpoch, @Nullable String recordId) {
    return of(message.toProto(), timestampMsSinceEpoch, recordId);
  }
}
