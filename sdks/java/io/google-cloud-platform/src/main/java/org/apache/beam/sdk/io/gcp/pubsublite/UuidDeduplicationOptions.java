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

import static com.google.cloud.pubsublite.internal.UncheckedApiPreconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsublite.internal.Uuid;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.Deduplicate;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;

/** Options for deduplicating Pub/Sub Lite messages based on the UUID they were published with. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class UuidDeduplicationOptions implements Serializable {
  private static final long serialVersionUID = 9837489720893L;

  public static final SerializableFunction<SequencedMessage, Uuid> DEFAULT_UUID_EXTRACTOR =
      message -> {
        checkArgument(
            message.getMessage().getAttributesMap().containsKey(Uuid.DEFAULT_ATTRIBUTE),
            "Uuid attribute missing.");
        List<ByteString> attributes =
            message.getMessage().getAttributesMap().get(Uuid.DEFAULT_ATTRIBUTE).getValuesList();
        checkArgument(attributes.size() == 1, "Duplicate Uuid attribute values exist.");
        return Uuid.of(attributes.get(0));
      };

  public static final TimeDomain DEFAULT_TIME_DOMAIN = TimeDomain.EVENT_TIME;
  public static final Duration DEFAULT_DEDUPLICATE_DURATION = Duration.standardDays(1);

  // All parameters are optional.
  public abstract SerializableFunction<SequencedMessage, Uuid> uuidExtractor();

  public abstract Deduplicate.KeyedValues<Uuid, SequencedMessage> deduplicate();

  @SuppressWarnings("CheckReturnValue")
  public static Builder newBuilder() {
    Builder builder = new AutoValue_UuidDeduplicationOptions.Builder();
    builder.setUuidExtractor(DEFAULT_UUID_EXTRACTOR);
    builder.setDeduplicate(
        Deduplicate.<Uuid, SequencedMessage>keyedValues()
            .withTimeDomain(DEFAULT_TIME_DOMAIN)
            .withDuration(DEFAULT_DEDUPLICATE_DURATION));
    return builder;
  }

  @CanIgnoreReturnValue
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUuidExtractor(
        SerializableFunction<SequencedMessage, Uuid> uuidExtractor);

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

    public abstract UuidDeduplicationOptions build();
  }
}
