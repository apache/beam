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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.TIMESTAMP_FIELD;

import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} to convert {@link Row} to {@link PubsubMessage} with JSON/AVRO payload.
 *
 * <p>Currently only supports writing a flat schema into a JSON/AVRO payload. This means that all
 * Row field values are written to the {@link PubsubMessage} payload, except for {@code
 * event_timestamp}, which is either ignored or written to the message attributes, depending on
 * whether config.getValue("timestampAttributeKey") is set.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class RowToPubsubMessage extends PTransform<PCollection<Row>, PCollection<PubsubMessage>> {
  private static final Logger LOG = LoggerFactory.getLogger(RowToPubsubMessage.class);
  private final boolean useTimestampAttribute;
  private final PayloadSerializer serializer;

  private RowToPubsubMessage(boolean useTimestampAttribute, PayloadSerializer serializer) {
    this.useTimestampAttribute = useTimestampAttribute;
    this.serializer = serializer;
  }

  public static RowToPubsubMessage of(boolean useTimestampAttribute, PayloadSerializer serializer) {
    return new RowToPubsubMessage(useTimestampAttribute, serializer);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<Row> input) {
    // If a timestamp attribute is used, make sure the TIMESTAMP_FIELD is propagated to the
    // element's event time. PubSubIO will populate the attribute from there.
    PCollection<Row> withTimestamp =
        useTimestampAttribute
            ? input.apply(WithTimestamps.of((row) -> row.getDateTime(TIMESTAMP_FIELD).toInstant()))
            : input;

    PCollection<Row> rows;
    if (withTimestamp.getSchema().hasField(TIMESTAMP_FIELD)) {
      if (!useTimestampAttribute) {
        // Warn the user if they're writing data to TIMESTAMP_FIELD, but event timestamp is mapped
        // to publish time. The data will be dropped.
        LOG.warn(
            String.format(
                "Dropping output field '%s' before writing to PubSub because this is a read-only "
                    + "column. To preserve this information you must configure a timestamp attribute.",
                TIMESTAMP_FIELD));
      }
      rows = withTimestamp.apply(DropFields.fields(TIMESTAMP_FIELD));
    } else {
      rows = withTimestamp;
    }

    return rows.apply(
            "MapRowToBytes",
            MapElements.into(new TypeDescriptor<byte[]>() {}).via(serializer::serialize))
        .apply("MapToPubsubMessage", MapElements.via(new ToPubsubMessage()));
  }

  private static class ToPubsubMessage extends SimpleFunction<byte[], PubsubMessage> {
    @Override
    public PubsubMessage apply(byte[] bytes) {
      return new PubsubMessage(bytes, ImmutableMap.of());
    }
  }
}
