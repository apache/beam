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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.DateTime;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;


/**
 *  When reading from Cloud Pub/Sub where record timestamps are provided as Pub/Sub message
 * attributes, specifies the way to extract timestamps from the message. This extraction
 * is either done by providing the name of the attribute that contains the timestamp,
 * or a function that takes a {@link String} payload, parses the payload, and returns a
 * timestamp.
 */
public class PubsubTimestampExtractor implements Serializable{

  /**
   * Interface for user to define a function that takes a Pubsub message payload and
   * extracts the timestamp.
   */
  public interface TimestampExtractorFn extends Serializable{

    /**
     * Extract the message timestamp out of the message payload.
     */
    String extractTimestamp(String messagePaylod);
  }

  /**
   * Attribute to use for custom timestamps, or {@literal null} if should use
   * {@code functionExtractor}. If {@code functionExtractor} is also null, use Pubsub publish time
   * instead.
   */
  @Nullable
  private String timestampAttribute;
  /**
   * Function extractor to use for custom timestamps from message payload, or {@literal null}
   * if should use {@code timestampAttribute}. If {@code timestampAttribute} is also null,
   * use Pubsub publish time instead.
   */
  @Nullable
  private TimestampExtractorFn functionExtractor;

  /**
   * Used to specify using Pubsub publish time as timestamp.
   */
  public PubsubTimestampExtractor() {

  }

  /**
   * Used to specify timestamp attribute as extraction method for timestamp.
   */
  public PubsubTimestampExtractor(String timestampAttribute) {
    this.timestampAttribute = timestampAttribute;
  }

  /**
   * Used to specify function from message payload to timestamp as extraction method for timestamp.
   */
  public PubsubTimestampExtractor(TimestampExtractorFn functionExtractor) {
    this.functionExtractor = functionExtractor;
  }

  /**
   * Whether this extractor uses a timestamp attribute to extract timestamps.
   */
  public boolean hasTimestampAttribute() {
    return !Strings.isNullOrEmpty(timestampAttribute);
  }

  /**
   * The timestamp attribute used to extract timestamps.
   */
  public String getTimestampAttribute() {
    return timestampAttribute;
  }

  /**
   * Return timestamp as ms-since-unix-epoch corresponding to {@code timestamp}.
   * Return {@literal null} if no timestamp could be found. Throw {@link IllegalArgumentException}
   * if timestamp cannot be recognized.
   */
  @Nullable
  private Long asMsSinceEpoch(@Nullable String timestamp) {
    if (Strings.isNullOrEmpty(timestamp)) {
      return null;
    }
    try {
      // Try parsing as milliseconds since epoch. Note there is no way to parse a
      // string in RFC 3339 format here.
      // Expected IllegalArgumentException if parsing fails; we use that to fall back
      // to RFC 3339.
      return Long.parseLong(timestamp);
    } catch (IllegalArgumentException e1) {
      // Try parsing as RFC3339 string. DateTime.parseRfc3339 will throw an
      // IllegalArgumentException if parsing fails, and the caller should handle.
      return DateTime.parseRfc3339(timestamp).getValue();
    }
  }

  /**
   * Return the timestamp (in ms since unix epoch) to use for a Pubsub message with payload
   * {@code message}, {@code attributes} and {@code pubsubTimestamp}.
   *
   * <p>If {@code timestampAttribute} is non-{@literal null} then the message attributes must
   * contain that attribute, and the value of that attribute will be taken as the timestamp.
   * Otherwise, if {@code functionExtractor} is non-{@literal null} then the message paylod must
   * contain the timestamp, which is parsed out by {@code functionExtractor}.
   * Otherwise the timestamp will be taken from the Pubsub publish timestamp {@code
   * pubsubTimestamp}.
   *
   * @throws IllegalArgumentException if the timestamp cannot be recognized as a ms-since-unix-epoch
   *     or RFC3339 time.
   */
  public long extractTimestamp(@Nullable String message, @Nullable String pubsubTimestamp,
      @Nullable Map<String, String> attributes) {
    Long timestampMsSinceEpoch;
    if (Strings.isNullOrEmpty(timestampAttribute)) {
      if (functionExtractor != null) {
        String timestampStr = functionExtractor.extractTimestamp(message);
        timestampMsSinceEpoch = asMsSinceEpoch(timestampStr);
        checkArgument(timestampMsSinceEpoch != null,
            "Cannot interpret timestamp from function extractor: %s", timestampStr);
      } else {
        timestampMsSinceEpoch = asMsSinceEpoch(pubsubTimestamp);
        checkArgument(timestampMsSinceEpoch != null,
            "Cannot interpret PubSub publish timestamp: %s", pubsubTimestamp);
      }
    } else {
      String value = attributes == null ? null : attributes.get(timestampAttribute);
      checkArgument(value != null, "PubSub message is missing a value for timestamp attribute %s",
          timestampAttribute);
      timestampMsSinceEpoch = asMsSinceEpoch(value);
      checkArgument(timestampMsSinceEpoch != null,
          "Cannot interpret value of attribute %s as timestamp: %s", timestampAttribute, value);
    }
    return timestampMsSinceEpoch;
  }

}
