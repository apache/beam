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
package org.apache.beam.io.iceberg;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.NestedField;

class RowHelper {

  // static helper functions only
  private RowHelper() {}

  public static Record rowToRecord(org.apache.iceberg.Schema schema, Row row) {
    return copy(GenericRecord.create(schema), row);
  }

  private static Record copy(Record baseRecord, Row value) {
    Record rec = baseRecord.copy();
    for (NestedField f : rec.struct().fields()) {
      copyInto(rec, f, value);
    }
    return rec;
  }

  private static void copyInto(Record rec, NestedField field, Row value) {
    String name = field.name();
    switch (field.type().typeId()) {
      case BOOLEAN:
        Optional.ofNullable(value.getBoolean(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case INTEGER:
        Optional.ofNullable(value.getInt32(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case LONG:
        Optional.ofNullable(value.getInt64(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case FLOAT:
        Optional.ofNullable(value.getFloat(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case DOUBLE:
        Optional.ofNullable(value.getDouble(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case DATE:
        throw new UnsupportedOperationException("Date fields not yet supported");
      case TIME:
        throw new UnsupportedOperationException("Time fields not yet supported");
      case TIMESTAMP:
        Optional.ofNullable(value.getDateTime(name))
            .ifPresent(v -> rec.setField(name, v.getMillis()));
        break;
      case STRING:
        Optional.ofNullable(value.getString(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case UUID:
        Optional.ofNullable(value.getBytes(name))
            .ifPresent(v -> rec.setField(name, UUID.nameUUIDFromBytes(v)));
        break;
      case FIXED:
        throw new UnsupportedOperationException("Fixed-precision fields are not yet supported.");
      case BINARY:
        Optional.ofNullable(value.getBytes(name))
            .ifPresent(v -> rec.setField(name, ByteBuffer.wrap(v)));
        break;
      case DECIMAL:
        Optional.ofNullable(value.getDecimal(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case STRUCT:
        Optional.ofNullable(value.getRow(name))
            .ifPresent(
                row ->
                    rec.setField(
                        name, copy(GenericRecord.create(field.type().asStructType()), row)));
        break;
      case LIST:
        throw new UnsupportedOperationException("List fields are not yet supported.");
      case MAP:
        throw new UnsupportedOperationException("Map fields are not yet supported.");
    }
  }
}
