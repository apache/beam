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

import static org.apache.beam.sdk.io.gcp.firestore.FirestoreHelper.makeStringValue;
import static org.apache.beam.sdk.io.gcp.firestore.FirestoreHelper.makeValue;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@code PTransform} to perform a conversion of {@link Row} to {@link Document}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RowToDocument extends PTransform<PCollection<Row>, PCollection<Document>> {
  private final String keyField;
  private static final Logger LOG = LoggerFactory.getLogger(RowToDocument.class);

  private RowToDocument(String keyField) {
    this.keyField = keyField;
  }

  @Override
  public PCollection<Document> expand(PCollection<Row> input) {
    if (!input
        .getSchema()
        .getField(keyField)
        .getType()
        .getTypeName()
        .equals(Schema.TypeName.STRING)) {
      throw new IllegalStateException(
          "Field `"
              + keyField
              + "` should of type `STRING`. Please change the type or specify a field to"
              + " write the KEY value from.");
    }
    LOG.info("Field to use as Document KEY is set to: `" + keyField + "`.");
    return input.apply(ParDo.of(new RowToDocument.RowToDocumentConverter()));
  }

  /**
   * Create a PTransform instance.
   *
   * @param keyField Row field containing a String key, must be set.
   * @return {@code PTransform} instance for Row to Document conversion.
   */
  public static RowToDocument create(String keyField) {
    return new RowToDocument(keyField);
  }

  class RowToDocumentConverter extends DoFn<Row, Document> {
    RowToDocumentConverter() {
      super();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();

      Schema schemaWithoutKeyField =
          Schema.builder()
              .addFields(
                  row.getSchema().getFields().stream()
                      .filter(field -> !field.getName().equals(keyField))
                      .collect(Collectors.toList()))
              .build();
      Document.Builder documentBuilder = Document.newBuilder();
      documentBuilder.putAllFields(constructMapFromRow(schemaWithoutKeyField, row).getFieldsMap());
      documentBuilder.setName(constructKeyFromRow(row));

      context.output(documentBuilder.build());
    }

    /**
     * Converts an entire {@code Row} to an appropriate Firestore {@code MapValue}.
     *
     * @param row {@code Row} to convert.
     * @return resulting {@code MapValue}.
     */
    private MapValue constructMapFromRow(Schema schema, Row row) {
      MapValue.Builder mapValueBuilder = MapValue.newBuilder();
      for (Schema.Field field : schema.getFields()) {
        Value val = mapObjectToValue(row.getValue(field.getName()));
        mapValueBuilder.putFields(field.getName(), val);
      }
      return mapValueBuilder.build();
    }

    /**
     * Create a random key for a {@code Row} without a keyField or use a user-specified key by
     * parsing it from byte array when keyField is set.
     *
     * @param row {@code Row} to construct a key for.
     * @return resulting {@code Key}.
     */
    private String constructKeyFromRow(Row row) {
      return row.getString(keyField);
    }

    /**
     * Converts a {@code Row} value to an appropriate Firestore {@code Value} object.
     *
     * @param value {@code Row} value to convert.
     * @return resulting {@code Value}.
     * @throws IllegalStateException when no mapping function for object of given type exists.
     */
    private Value mapObjectToValue(Object value) {
      if (value == null) {
        return Value.newBuilder().build();
      }

      if (Boolean.class.equals(value.getClass())) {
        return makeValue((Boolean) value).build();
      } else if (Byte.class.equals(value.getClass())) {
        return makeValue((Byte) value).build();
      } else if (Long.class.equals(value.getClass())) {
        return makeValue((Long) value).build();
      } else if (Short.class.equals(value.getClass())) {
        return makeValue((Short) value).build();
      } else if (Integer.class.equals(value.getClass())) {
        return makeValue((Integer) value).build();
      } else if (Double.class.equals(value.getClass())) {
        return makeValue((Double) value).build();
      } else if (Float.class.equals(value.getClass())) {
        return makeValue((Float) value).build();
      } else if (String.class.equals(value.getClass())) {
        return makeStringValue((String) value).build();
      } else if (Instant.class.equals(value.getClass())) {
        return makeValue(((Instant) value).toDate()).build();
      } else if (byte[].class.equals(value.getClass())) {
        return makeValue(ByteString.copyFrom((byte[]) value)).build();
      } else if (value instanceof Row) {
        // Recursive conversion to handle nested rows.
        Row row = (Row) value;
        return makeValue(constructMapFromRow(row.getSchema(), row)).build();
      } else if (value instanceof Collection) {
        // Recursive to handle nested collections.
        Collection<Object> collection = (Collection<Object>) value;
        List<Value> arrayValues =
            collection.stream().map(this::mapObjectToValue).collect(Collectors.toList());
        return makeValue(arrayValues).build();
      }
      throw new IllegalStateException(
          "No conversion exists from type: " + value.getClass() + " to Firestore Value.");
    }
  }
}
