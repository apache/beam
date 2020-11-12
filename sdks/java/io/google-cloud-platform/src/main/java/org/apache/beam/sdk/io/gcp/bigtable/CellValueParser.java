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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.bigtable.v2.Cell;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Shorts;
import org.joda.time.DateTime;

class CellValueParser implements Serializable {

  Object getCellValue(Cell cell, Schema.TypeName type) {
    ByteString cellValue = cell.getValue();
    int valueSize = cellValue.size();
    switch (type) {
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
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported cell value type '%s'.", type));
    }
  }

  private String message(String type, int byteSize) {
    return String.format(
        "%s has to be %s-byte%s long bytearray", type, byteSize, byteSize == 1 ? "" : "s");
  }
}
