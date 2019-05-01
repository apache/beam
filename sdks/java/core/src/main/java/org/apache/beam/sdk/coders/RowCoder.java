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
package org.apache.beam.sdk.coders;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

/** A {@link Coder} for {@link Row}. It wraps the {@link Coder} for each element directly. */
@Experimental
public class RowCoder extends CustomCoder<Row> {
  // This contains a map of primitive types to their coders.
  static final ImmutableMap<TypeName, Coder> CODER_MAP =
      ImmutableMap.<TypeName, Coder>builder()
          .put(TypeName.BYTE, ByteCoder.of())
          .put(TypeName.BYTES, ByteArrayCoder.of())
          .put(TypeName.INT16, BigEndianShortCoder.of())
          .put(TypeName.INT32, VarIntCoder.of())
          .put(TypeName.INT64, VarLongCoder.of())
          .put(TypeName.DECIMAL, BigDecimalCoder.of())
          .put(TypeName.FLOAT, FloatCoder.of())
          .put(TypeName.DOUBLE, DoubleCoder.of())
          .put(TypeName.STRING, StringUtf8Coder.of())
          .put(TypeName.DATETIME, InstantCoder.of())
          .put(TypeName.BOOLEAN, BooleanCoder.of())
          .build();

  private static final ImmutableMap<TypeName, Integer> ESTIMATED_FIELD_SIZES =
      ImmutableMap.<TypeName, Integer>builder()
          .put(TypeName.BYTE, Byte.BYTES)
          .put(TypeName.INT16, Short.BYTES)
          .put(TypeName.INT32, Integer.BYTES)
          .put(TypeName.INT64, Long.BYTES)
          .put(TypeName.FLOAT, Float.BYTES)
          .put(TypeName.DOUBLE, Double.BYTES)
          .put(TypeName.DECIMAL, 32)
          .put(TypeName.BOOLEAN, 1)
          .put(TypeName.DATETIME, Long.BYTES)
          .build();

  private final Schema schema;

  public UUID getId() {
    return id;
  }

  private final UUID id;
  @Nullable private transient Coder<Row> delegateCoder = null;

  public static RowCoder of(Schema schema) {
    UUID id = (schema.getUUID() == null) ? UUID.randomUUID() : schema.getUUID();
    return new RowCoder(schema, id);
  }

  private RowCoder(Schema schema, UUID id) {
    if (schema.getUUID() != null) {
      checkArgument(
          schema.getUUID().equals(id),
          "Schema has a UUID that doesn't match argument to constructor. %s v.s. %s",
          schema.getUUID(),
          id);
    } else {
      // Clone the schema before modifying the Java object.
      schema = SerializableUtils.clone(schema);
      setSchemaIds(schema, id);
    }
    this.schema = schema;
    this.id = id;
  }

  // Sets the schema id, and then recursively ensures that all schemas have ids set.
  private void setSchemaIds(Schema schema, UUID id) {
    if (schema.getUUID() == null) {
      schema.setUUID(id);
    }
    for (Field field : schema.getFields()) {
      setSchemaIds(field.getType());
    }
  }

  private void setSchemaIds(FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ROW:
        setSchemaIds(fieldType.getRowSchema(), UUID.randomUUID());
        return;
      case MAP:
        setSchemaIds(fieldType.getMapKeyType());
        setSchemaIds(fieldType.getMapValueType());
        return;
      case LOGICAL_TYPE:
        setSchemaIds(fieldType.getLogicalType().getBaseType());
        return;

      case ARRAY:
        setSchemaIds(fieldType.getCollectionElementType());
        return;

      default:
        return;
    }
  }

  // Return the generated coder class for this schema.
  private Coder<Row> getDelegateCoder() {
    if (delegateCoder == null) {
      // RowCoderGenerator caches based on id, so if a new instance of this RowCoder is
      // deserialized, we don't need to run ByteBuddy again to construct the class.
      delegateCoder = RowCoderGenerator.generate(schema);
    }
    return delegateCoder;
  }

  @Override
  public void encode(Row value, OutputStream outStream) throws IOException {
    getDelegateCoder().encode(value, outStream);
  }

  @Override
  public Row decode(InputStream inStream) throws IOException {
    return getDelegateCoder().decode(inStream);
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
    verifyDeterministic(schema);
  }

  private void verifyDeterministic(Schema schema)
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {

    List<Coder<?>> coders =
        schema.getFields().stream()
            .map(Field::getType)
            .map(RowCoder::coderForFieldType)
            .collect(Collectors.toList());

    Coder.verifyDeterministic(this, "All fields must have deterministic encoding", coders);
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  /** Returns the coder used for a given primitive type. */
  public static <T> Coder<T> coderForFieldType(FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ROW:
        return (Coder<T>) RowCoder.of(fieldType.getRowSchema());
      case ARRAY:
        return (Coder<T>) ListCoder.of(coderForFieldType(fieldType.getCollectionElementType()));
      case MAP:
        return (Coder<T>)
            MapCoder.of(
                coderForFieldType(fieldType.getMapKeyType()),
                coderForFieldType(fieldType.getMapValueType()));
      default:
        return (Coder<T>) CODER_MAP.get(fieldType.getTypeName());
    }
  }

  /** Return the estimated serialized size of a give row object. */
  public static long estimatedSizeBytes(Row row) {
    Schema schema = row.getSchema();
    int fieldCount = schema.getFieldCount();
    int bitmapSize = (((fieldCount - 1) >> 6) + 1) * 8;

    int fieldsSize = 0;
    for (int i = 0; i < schema.getFieldCount(); ++i) {
      fieldsSize += (int) estimatedSizeBytes(schema.getField(i).getType(), row.getValue(i));
    }
    return (long) bitmapSize + fieldsSize;
  }

  private static long estimatedSizeBytes(FieldType typeDescriptor, Object value) {
    switch (typeDescriptor.getTypeName()) {
      case LOGICAL_TYPE:
        return estimatedSizeBytes(typeDescriptor.getLogicalType().getBaseType(), value);
      case ROW:
        return estimatedSizeBytes((Row) value);
      case ARRAY:
        List list = (List) value;
        long listSizeBytes = 0;
        for (Object elem : list) {
          listSizeBytes += estimatedSizeBytes(typeDescriptor.getCollectionElementType(), elem);
        }
        return 4 + listSizeBytes;
      case BYTES:
        byte[] bytes = (byte[]) value;
        return 4L + bytes.length;
      case MAP:
        Map<Object, Object> map = (Map<Object, Object>) value;
        long mapSizeBytes = 0;
        for (Map.Entry<Object, Object> elem : map.entrySet()) {
          mapSizeBytes +=
              typeDescriptor.getMapKeyType().getTypeName().equals(TypeName.STRING)
                  ? ((String) elem.getKey()).length()
                  : ESTIMATED_FIELD_SIZES.get(typeDescriptor.getMapKeyType().getTypeName());
          mapSizeBytes += estimatedSizeBytes(typeDescriptor.getMapValueType(), elem.getValue());
        }
        return 4 + mapSizeBytes;
      case STRING:
        // Not always accurate - String.getBytes().length() would be more accurate here, but slower.
        return ((String) value).length();
      default:
        return ESTIMATED_FIELD_SIZES.get(typeDescriptor.getTypeName());
    }
  }

  @Override
  public String toString() {
    String string = "Schema: " + schema + "  UUID: " + id + " delegateCoder: " + getDelegateCoder();
    return string;
  }
}
