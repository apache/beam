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
package org.apache.beam.sdk.io.gcp.bigtable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.bigtable.v2.Cell;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Shorts;
import org.joda.time.DateTime;

class CellValueParser implements Serializable {

  Object getCellValue(Cell cell, Schema.FieldType type) {
    ByteString cellValue = cell.getValue();
    int valueSize = cellValue.size();
    switch (type.getTypeName()) {
      case BOOLEAN:
        checkArgument(valueSize == 1, message("Boolean", 1));
        return cellValue.toByteArray()[0] != 0;
      case BYTE:
        checkArgument(valueSize == 1, message("Byte", 1));
        return cellValue.toByteArray()[0];
      case INT16:
        checkArgument(valueSize == 2, message("Int16", 2));
        return Shorts.fromByteArray(cellValue.toByteArray());
      case INT32:
        checkArgument(valueSize == 4, message("Int32", 4));
        return Ints.fromByteArray(cellValue.toByteArray());
      case INT64:
        checkArgument(valueSize == 8, message("Int64", 8));
        return Longs.fromByteArray(cellValue.toByteArray());
      case FLOAT:
        checkArgument(valueSize == 4, message("Float", 4));
        return Float.intBitsToFloat(Ints.fromByteArray(cellValue.toByteArray()));
      case DOUBLE:
        checkArgument(valueSize == 8, message("Double", 8));
        return Double.longBitsToDouble(Longs.fromByteArray(cellValue.toByteArray()));
      case DATETIME:
        return DateTime.parse(cellValue.toStringUtf8());
      case STRING:
        return cellValue.toStringUtf8();
      case BYTES:
        return cellValue.toByteArray();
      case LOGICAL_TYPE:
        String identifier = checkArgumentNotNull(type.getLogicalType()).getIdentifier();
        throw new IllegalStateException("Unsupported logical type: " + identifier);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported cell value type '%s'.", type.getTypeName()));
    }
  }

  ByteString valueToByteString(Object value, Schema.FieldType type) {
    switch (type.getTypeName()) {
      case BOOLEAN:
        return byteString(((Boolean) value) ? new byte[] {1} : new byte[] {0});
      case FLOAT:
        return byteString(Ints.toByteArray(Float.floatToIntBits((Float) value)));
      case DOUBLE:
        return byteString(Longs.toByteArray(Double.doubleToLongBits((Double) value)));
      case BYTE:
        return byteString(new byte[] {(Byte) value});
      case INT16:
        return byteString(Shorts.toByteArray((Short) value));
      case INT32:
        return byteString(Ints.toByteArray((Integer) value));
      case INT64:
        return byteString(Longs.toByteArray((Long) value));
      case STRING:
        return byteString(((String) value).getBytes(UTF_8));
      case BYTES:
        return byteString((byte[]) value);
      case DATETIME:
        return byteString(value.toString().getBytes(UTF_8));
      case LOGICAL_TYPE:
        LogicalType<?, ?> logicalType = checkArgumentNotNull(type.getLogicalType());
        if (logicalType instanceof PassThroughLogicalType) {
          return valueToByteString(value, logicalType.getBaseType());
        } else {
          throw new IllegalStateException(
              "Unsupported logical type: " + logicalType.getIdentifier());
        }
      default:
        throw new IllegalStateException("Unsupported type: " + type.getTypeName());
    }
  }

  private ByteString byteString(byte[] value) {
    return ByteString.copyFrom(value);
  }

  private String message(String type, int byteSize) {
    return String.format(
        "%s has to be %s-byte%s long bytearray", type, byteSize, byteSize == 1 ? "" : "s");
  }
}
