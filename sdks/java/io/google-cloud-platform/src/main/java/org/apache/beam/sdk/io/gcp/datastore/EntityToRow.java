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
package org.apache.beam.sdk.io.gcp.datastore;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@code PTransform} to perform a conversion of {@link Entity} to {@link Row}. */
public class EntityToRow extends PTransform<PCollection<Entity>, PCollection<Row>> {
  private final Schema schema;
  private final String keyField;
  private static final Logger LOG = LoggerFactory.getLogger(DataStoreV1SchemaIOProvider.class);

  private EntityToRow(Schema schema, String keyField) {
    this.schema = schema;
    this.keyField = keyField;

    if (schema.getFieldNames().contains(keyField)) {
      if (!schema.getField(keyField).getType().getTypeName().equals(Schema.TypeName.BYTES)) {
        throw new IllegalStateException(
            "Field `"
                + keyField
                + "` should of type `BYTES`. Please change the type or specify a field to"
                + " store the KEY value.");
      }
      LOG.info("Entity KEY will be stored under `" + keyField + "` field.");
    }
  }

  /**
   * Create a PTransform instance.
   *
   * @param schema {@code Schema} of the target row.
   * @param keyField A name of the row field to store the {@code Key} in.
   * @return {@code PTransform} instance for Entity to Row conversion.
   */
  public static EntityToRow create(Schema schema, String keyField) {
    return new EntityToRow(schema, keyField);
  }

  @Override
  public PCollection<Row> expand(PCollection<Entity> input) {
    return input.apply(ParDo.of(new EntityToRow.EntityToRowConverter())).setRowSchema(schema);
  }

  class EntityToRowConverter extends DoFn<Entity, Row> {

    @DoFn.ProcessElement
    public void processElement(ProcessContext context) {
      Entity entity = context.element();
      ImmutableMap.Builder<String, Value> mapBuilder = ImmutableMap.builder();
      mapBuilder.put(keyField, makeValue(entity.getKey()).build());
      mapBuilder.putAll(entity.getPropertiesMap());

      context.output(extractRowFromProperties(schema, mapBuilder.build()));
    }

    /**
     * Convert DataStore {@code Value} to Beam type.
     *
     * @param currentFieldType Beam {@code Schema.FieldType} to convert to (used for {@code Row} and
     *     {@code Array}).
     * @param val DataStore {@code Value}.
     * @return resulting Beam type.
     */
    private Object convertValueToObject(Schema.FieldType currentFieldType, Value val) {
      Value.ValueTypeCase typeCase = val.getValueTypeCase();

      switch (typeCase) {
        case NULL_VALUE:
        case VALUETYPE_NOT_SET:
          return null;
        case BOOLEAN_VALUE:
          return val.getBooleanValue();
        case INTEGER_VALUE:
          return val.getIntegerValue();
        case DOUBLE_VALUE:
          return val.getDoubleValue();
        case TIMESTAMP_VALUE:
          com.google.protobuf.Timestamp time = val.getTimestampValue();
          long millis = time.getSeconds() * 1000 + time.getNanos() / 1000;
          return Instant.ofEpochMilli(millis).toDateTime();
        case STRING_VALUE:
          return val.getStringValue();
        case KEY_VALUE:
          return val.getKeyValue().toByteArray();
        case BLOB_VALUE:
          return val.getBlobValue().toByteArray();
        case ENTITY_VALUE:
          // Recursive mapping for row type.
          Schema rowSchema = currentFieldType.getRowSchema();
          assert rowSchema != null;
          Entity entity = val.getEntityValue();
          return extractRowFromProperties(rowSchema, entity.getPropertiesMap());
        case ARRAY_VALUE:
          // Recursive mapping for collection type.
          Schema.FieldType elementType = currentFieldType.getCollectionElementType();
          List<Value> valueList = val.getArrayValue().getValuesList();
          return valueList.stream()
              .map(v -> convertValueToObject(elementType, v))
              .collect(Collectors.toList());
        case GEO_POINT_VALUE:
        default:
          throw new IllegalStateException(
              "No conversion exists from type: "
                  + val.getValueTypeCase().name()
                  + " to Beam type.");
      }
    }

    /**
     * Converts all properties of an {@code Entity} to Beam {@code Row}.
     *
     * @param schema Target row {@code Schema}.
     * @param values A map of property names and values.
     * @return resulting Beam {@code Row}.
     */
    private Row extractRowFromProperties(Schema schema, Map<String, Value> values) {
      Row.Builder builder = Row.withSchema(schema);
      // It is not a guarantee that the values will be in the same order as the schema.
      // Maybe metadata:
      // https://cloud.google.com/appengine/docs/standard/python/datastore/metadataqueries
      // TODO: figure out in what order the elements are in (without relying on Beam schema).
      for (Schema.Field field : schema.getFields()) {
        Value val = values.get(field.getName());
        builder.addValue(convertValueToObject(field.getType(), val));
      }
      return builder.build();
    }
  }
}
