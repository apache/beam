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
package org.apache.beam.sdk.extensions.sql.meta.provider.datagen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

/** The main PTransform that encapsulates the data generation logic. */
public class DataGeneratorPTransform extends PTransform<PBegin, PCollection<Row>> {
  private final Schema schema;
  private final ObjectNode properties;

  public DataGeneratorPTransform(Schema schema, ObjectNode properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    GenerateSequence generator;
    JsonNode rpsNode = properties.path("rows-per-second");
    JsonNode numRowsNode = properties.path("number-of-rows");

    if (!rpsNode.isMissingNode()) {
      generator = GenerateSequence.from(0).withRate(rpsNode.asLong(), Duration.standardSeconds(1));
    } else if (!numRowsNode.isMissingNode()) {
      generator = GenerateSequence.from(0).to(numRowsNode.asLong());
    } else {
      throw new IllegalArgumentException(
          "A 'datagen' table requires either 'rows-per-second' (for unbounded) or "
              + "'number-of-rows' (for bounded) in TBLPROPERTIES.");
    }

    String behavior = properties.path("timestamp.behavior").asText("processing-time");
    @Nullable String eventTimeColumn = null;

    if ("event-time".equalsIgnoreCase(behavior)) {
      JsonNode columnNode = properties.path("event-time.timestamp-column");

      if (columnNode.isMissingNode() || columnNode.isNull()) {
        throw new IllegalArgumentException(
            "For 'event-time' behavior, 'event-time.timestamp-column' must be specified.");
      }
      eventTimeColumn = columnNode.asText();

      // Validate that the specified column exists and is of type TIMESTAMP.
      if (!schema.hasField(eventTimeColumn)) {
        throw new IllegalArgumentException(
            String.format(
                "The specified 'event-time.timestamp-column' ('%s') does not exist in the table schema.",
                eventTimeColumn));
      }

      Schema.Field eventTimeField = schema.getField(eventTimeColumn);
      if (!Schema.TypeName.DATETIME.equals(eventTimeField.getType().getTypeName())) {
        throw new IllegalArgumentException(
            String.format(
                "The specified 'event-time.timestamp-column' ('%s') must be of type TIMESTAMP, but was '%s'.",
                eventTimeColumn, eventTimeField.getType()));
      }

      long maxOutOfOrdernessMs = properties.path("event_time.max-out-of-orderness").asLong(0L);
      generator = generator.withTimestampFn(new AdvancingTimestampFn(maxOutOfOrdernessMs));
    }

    return input
        .getPipeline()
        .apply("GenerateSequence", generator)
        .apply(
            "GenerateRows", ParDo.of(new DataGeneratorRowFn(schema, properties, eventTimeColumn)))
        .setRowSchema(schema);
  }
}
