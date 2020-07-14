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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static com.google.cloud.pubsublite.internal.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.Deduplicate;
import org.joda.time.Duration;

/** Options for deduplicating Pub/Sub Lite messages based on the UUID they were published with. */
@AutoValue
public abstract class UuidDeduplicationOptions implements Serializable {
  private static final long serialVersionUID = 9837489720893L;

  public static final SerializableStatusFunction<SequencedMessage, Uuid> DEFAULT_UUID_EXTRACTOR =
      message -> {
        checkArgument(
            message.getMessage().getAttributesMap().containsKey(Uuid.DEFAULT_ATTRIBUTE),
            "Uuid attribute missing.");
        List<ByteString> attributes =
            message.getMessage().getAttributesMap().get(Uuid.DEFAULT_ATTRIBUTE).getValuesList();
        checkArgument(attributes.size() == 1, "Duplicate Uuid attribute values exist.");
        return Uuid.of(attributes.get(0));
      };

  public static final int DEFAULT_HASH_PARTITIONS = 10000;

  public static final TimeDomain DEFAULT_TIME_DOMAIN = TimeDomain.EVENT_TIME;
  public static final Duration DEFAULT_DEDUPLICATE_DURATION = Duration.standardDays(1);

  // All parameters are optional.
  public abstract SerializableStatusFunction<SequencedMessage, Uuid> uuidExtractor();

  public abstract Deduplicate.KeyedValues<Uuid, SequencedMessage> deduplicate();

  // The number of partitions to hash values into.
  public abstract int hashPartitions();

  @SuppressWarnings("CheckReturnValue")
  public static Builder newBuilder() {
    Builder builder = new AutoValue_UuidDeduplicationOptions.Builder();
    builder.setUuidExtractor(DEFAULT_UUID_EXTRACTOR);
    builder.setDeduplicate(
        Deduplicate.<Uuid, SequencedMessage>keyedValues()
            .withTimeDomain(DEFAULT_TIME_DOMAIN)
            .withDuration(DEFAULT_DEDUPLICATE_DURATION));
    builder.setHashPartitions(DEFAULT_HASH_PARTITIONS);
    return builder;
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUuidExtractor(
        SerializableStatusFunction<SequencedMessage, Uuid> uuidExtractor);

    /**
     * Set the deduplication transform.
     *
     * <pre>{@code
     * UuidDeduplicationOptions.Builder builder = UuidDeduplicationOptions.newBuilder();
     * builder.setDeduplicate(Deduplicate.<Uuid, SequencedMessage>keyedValues()
     *     .withTimeDomain(TimeDomain.PROCESSING_TIME));
     * }</pre>
     */
    public abstract Builder setDeduplicate(
        Deduplicate.KeyedValues<Uuid, SequencedMessage> deduplicate);

    public abstract Builder setHashPartitions(int partitions);

    public abstract UuidDeduplicationOptions build();
  }
}
