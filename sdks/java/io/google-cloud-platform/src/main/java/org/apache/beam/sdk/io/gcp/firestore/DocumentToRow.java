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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.apache.beam.sdk.io.gcp.firestore.FirestoreHelper.makeReferenceValue;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.util.Timestamps;
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

/** A {@code PTransform} to perform a conversion of {@link Document} to {@link Row}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DocumentToRow extends PTransform<PCollection<Document>, PCollection<Row>> {
  private final Schema schema;
  private final String keyField;
  private static final Logger LOG = LoggerFactory.getLogger(DocumentToRow.class);

  private DocumentToRow(Schema schema, String keyField) {
    this.schema = schema;
    this.keyField = keyField;

    if (schema.getFieldNames().contains(keyField)) {
      if (!schema.getField(keyField).getType().getTypeName().equals(Schema.TypeName.STRING)) {
        throw new IllegalStateException(
            "Field `"
                + keyField
                + "` should of type `STRING`. Please change the type or specify a field to"
                + " store the KEY value.");
      }
      LOG.info("Document KEY will be stored under `" + keyField + "` field.");
    }
  }

  /**
   * Create a PTransform instance.
   *
   * @param schema {@code Schema} of the target row.
   * @param keyField A name of the row field to store the {@code Key} in.
   * @return {@code PTransform} instance for Document to Row conversion.
   */
  public static DocumentToRow create(Schema schema, String keyField) {
    return new DocumentToRow(schema, keyField);
  }

  @Override
  public PCollection<Row> expand(PCollection<Document> input) {
    return input.apply(ParDo.of(new DocumentToRow.DocumentToRowConverter())).setRowSchema(schema);
  }

  class DocumentToRowConverter extends DoFn<Document, Row> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      Document document = context.element();
      ImmutableMap.Builder<String, Value> mapBuilder = ImmutableMap.builder();
      mapBuilder.put(keyField, makeReferenceValue(document.getName()).build());
      mapBuilder.putAll(document.getFieldsMap());

      context.output(extractRowFromProperties(schema, mapBuilder.build()));
    }

    /**
     * Convert Firestore {@code Value} to Beam type.
     *
     * @param currentFieldType Beam {@code Schema.FieldType} to convert to (used for {@code Row} and
     *     {@code Array}).
     * @param val Firestore {@code Value}.
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
          long millis = Timestamps.toMillis(time);
          return Instant.ofEpochMilli(millis).toDateTime();
        case STRING_VALUE:
          return val.getStringValue();
        case REFERENCE_VALUE:
          return val.getReferenceValue();
        case BYTES_VALUE:
          return val.getBytesValue().toByteArray();
        case MAP_VALUE:
          // Recursive mapping for row type.
          Schema rowSchema = currentFieldType.getRowSchema();
          assert rowSchema != null;
          MapValue map = val.getMapValue();
          return extractRowFromProperties(rowSchema, map.getFieldsMap());
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
     * Converts all properties of an {@code Document} to Beam {@code Row}.
     *
     * @param schema Target row {@code Schema}.
     * @param values A map of property names and values.
     * @return resulting Beam {@code Row}.
     */
    private Row extractRowFromProperties(Schema schema, Map<String, Value> values) {
      Row.Builder builder = Row.withSchema(schema);
      // It is not a guarantee that the values will be in the same order as the schema.
      for (Schema.Field field : schema.getFields()) {
        Value val = values.get(field.getName());
        builder.addValue(convertValueToObject(field.getType(), val));
      }
      return builder.build();
    }
  }
}
