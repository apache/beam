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
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A stateful DoFn that converts a sequence of Longs into structured Rows. */
public class DataGeneratorRowFn extends DoFn<Long, Row> {
  private final Schema schema;
  private final ObjectNode properties;
  private final @Nullable String primaryTimestampField;

  private transient Map<String, FieldGenerator> fieldGenerators;

  @SuppressWarnings("initialization")
  public DataGeneratorRowFn(
      Schema schema, ObjectNode properties, @Nullable String primaryTimestampField) {
    this.schema = schema;
    this.properties = properties;
    this.primaryTimestampField = primaryTimestampField;
  }

  @Setup
  public void setup() {
    this.fieldGenerators = new HashMap<>();

    for (Schema.Field field : schema.getFields()) {
      fieldGenerators.put(field.getName(), createGeneratorForField(field));
    }
  }

  @ProcessElement
  public void processElement(
      @Element Long index, @Timestamp Instant timestamp, OutputReceiver<Row> out) {
    Row.Builder rowBuilder = Row.withSchema(schema);
    for (Schema.Field field : schema.getFields()) {
      Object value;
      if (field.getName().equals(this.primaryTimestampField)) {
        value = timestamp.toDateTime();
      } else {
        FieldGenerator generator = fieldGenerators.get(field.getName());
        if (generator == null) {
          throw new IllegalStateException("No generator found for field: " + field.getName());
        }
        value = generator.generate(index);
      }
      rowBuilder.addValue(value);
    }
    out.output(rowBuilder.build());
  }

  @FunctionalInterface
  private interface FieldGenerator extends Serializable {
    @Nullable
    Object generate(long index);
  }

  private FieldGenerator createGeneratorForField(Schema.Field field) {
    String fieldName = field.getName();
    FieldGenerator valueGenerator = createValueGeneratorForField(field);
    double nullRate = properties.path("fields." + fieldName + ".null-rate").asDouble(0.0);

    if (nullRate > 0) {
      return (index) ->
          ThreadLocalRandom.current().nextDouble() < nullRate
              ? null
              : valueGenerator.generate(index);
    }
    return valueGenerator;
  }

  private FieldGenerator createValueGeneratorForField(Schema.Field field) {
    String fieldName = field.getName();
    String kind = properties.path("fields." + fieldName + ".kind").asText("random");

    final SqlTypeName sqlTypeName = CalciteUtils.toSqlTypeName(field.getType());
    if (sqlTypeName == null) {
      throw new UnsupportedOperationException(
          "Data generator requires a defined SQL type. Beam type '"
              + field.getType().getTypeName()
              + "' on field '"
              + field.getName()
              + "' is not supported.");
    }

    if ("sequence".equalsIgnoreCase(kind)) {
      if (!SqlTypeName.INT_TYPES.contains(sqlTypeName)) {
        throw new IllegalArgumentException(
            String.format(
                "The 'sequence' generator for integers only supports integer types, but field '%s' is of type '%s'.",
                field.getName(), sqlTypeName));
      }

      JsonNode startNode = properties.path("fields." + fieldName + ".start");
      JsonNode endNode = properties.path("fields." + fieldName + ".end");

      if (startNode.isMissingNode() && endNode.isMissingNode()) {
        return (index) -> index;
      }

      if (startNode.isMissingNode() || endNode.isMissingNode()) {
        throw new IllegalArgumentException(
            "For a cycling sequence generator, both 'start' and 'end' must be specified.");
      }

      long start = startNode.asLong(0L);
      long end = endNode.asLong(Long.MAX_VALUE);

      if (start > end) {
        throw new IllegalArgumentException(
            String.format(
                "For sequence generator, 'start' (%d) cannot be greater than 'end' (%d).",
                start, end));
      }
      long cycleLength = end - start + 1;
      switch (sqlTypeName) {
        case INTEGER:
          return (index) -> (int) (start + (index % cycleLength));
        case SMALLINT:
          return (index) -> (short) (start + (index % cycleLength));
        case TINYINT:
          return (index) -> (byte) (start + (index % cycleLength));
        default: // BIGINT
          return (index) -> start + (index % cycleLength);
      }
    }

    switch (sqlTypeName) {
      case CHAR:
      case VARCHAR:
        int length = properties.path("fields." + fieldName + ".length").asInt(10);
        return (index) -> RandomStringUtils.randomAlphanumeric(length);
      case BOOLEAN:
        return (index) -> ThreadLocalRandom.current().nextBoolean();
      case FLOAT:
      case DOUBLE:
        double minD = properties.path("fields." + fieldName + ".min").asDouble(0.0);
        double maxD = properties.path("fields." + fieldName + ".max").asDouble(1.0);
        return (index) -> minD + (maxD - minD) * ThreadLocalRandom.current().nextDouble();
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        long minL = properties.path("fields." + fieldName + ".min").asLong(0L);
        long maxL = properties.path("fields." + fieldName + ".max").asLong(Long.MAX_VALUE);
        return (index) -> minL + (long) (ThreadLocalRandom.current().nextDouble() * (maxL - minL));
      case DECIMAL:
        double minBd = properties.path("fields." + fieldName + ".min").asDouble(0.0);
        double maxBd = properties.path("fields." + fieldName + ".max").asDouble(1000.0);
        return (index) ->
            BigDecimal.valueOf(minBd + (maxBd - minBd) * ThreadLocalRandom.current().nextDouble());
      case TIMESTAMP:
        JsonNode maxPastNode = properties.path("fields." + fieldName + ".max-past");
        if (!maxPastNode.isMissingNode()) {
          long maxPastMs = maxPastNode.asLong();
          if (maxPastMs <= 0) {
            throw new IllegalArgumentException("'max-past' must be a positive long value.");
          }
          return (index) ->
              Instant.now()
                  .minus(
                      Duration.millis(
                          (long) (ThreadLocalRandom.current().nextDouble() * maxPastMs)));
        }
        return (index) -> Instant.now();
      default:
        throw new UnsupportedOperationException("Unsupported SQL type for datagen: " + sqlTypeName);
    }
  }
}
