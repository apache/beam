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

import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds a timestamp attribute if desired and filters it out of the underlying row if no timestamp
 * attribute exists.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class AddTimestampAttribute extends PTransform<PCollection<Row>, PCollection<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(AddTimestampAttribute.class);
  private final boolean useTimestampAttribute;

  AddTimestampAttribute(boolean useTimestampAttribute) {
    this.useTimestampAttribute = useTimestampAttribute;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
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

    return rows;
  }
}
