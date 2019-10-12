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
package org.apache.beam.sdk.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;

public class RowJsonSerializer extends StdSerializer<Row> {

  private final Schema schema;

  /** Creates a serializer for a {@link Row} {@link Schema}. */
  public static RowJsonSerializer forSchema(Schema schema) {
    schema.getFields().forEach(RowJsonValidation::verifyFieldTypeSupported);
    return new RowJsonSerializer(schema);
  }

  private RowJsonSerializer(Schema schema) {
    super(Row.class);
    this.schema = schema;
  }

  @Override
  public void serialize(Row value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    writeRow(value, this.schema, gen);
  }

  // TODO: ByteBuddy generate based on schema?
  private void writeRow(Row row, Schema schema, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    for (int i = 0; i < schema.getFieldCount(); ++i) {
      Field field = schema.getField(i);
      Object value = row.getValue(i);
      gen.writeFieldName(field.getName());
      if (field.getType().getNullable() && value == null) {
        gen.writeNull();
        continue;
      }
      writeValue(gen, field.getType(), value);
    }
    gen.writeEndObject();
  }

  private void writeValue(JsonGenerator gen, FieldType type, Object value) throws IOException {
    switch (type.getTypeName()) {
      case BOOLEAN:
        gen.writeBoolean((boolean) value);
        break;
      case STRING:
        gen.writeString((String) value);
        break;
      case BYTE:
        gen.writeNumber((byte) value);
        break;
      case DOUBLE:
        gen.writeNumber((double) value);
        break;
      case FLOAT:
        gen.writeNumber((float) value);
        break;
      case INT16:
        gen.writeNumber((short) value);
        break;
      case INT32:
        gen.writeNumber((int) value);
        break;
      case INT64:
        gen.writeNumber((long) value);
        break;
      case DECIMAL:
        gen.writeNumber((BigDecimal) value);
        break;
      case ARRAY:
        gen.writeStartArray();
        for (Object element : (List<Object>) value) {
          writeValue(gen, type.getCollectionElementType(), element);
        }
        gen.writeEndArray();
        break;
      case ROW:
        writeRow((Row) value, type.getRowSchema(), gen);
        break;
      default:
        throw new IllegalArgumentException("Unsupported field type: " + type);
    }
  }
}
