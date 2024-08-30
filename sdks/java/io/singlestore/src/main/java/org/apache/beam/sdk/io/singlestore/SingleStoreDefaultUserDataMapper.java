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
package org.apache.beam.sdk.io.singlestore;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * UserDataMapper that maps {@link Row} objects. ARRAYs, ITTERABLEs, MAPs and nested ROWs are not
 * supported.
 */
final class SingleStoreDefaultUserDataMapper implements SingleStoreIO.UserDataMapper<Row> {

  private final transient DateTimeFormatter formatter =
      DateTimeFormat.forPattern("yyyy-MM-DD' 'HH:mm:ss.SSS");

  private String convertLogicalTypeFieldToString(Schema.FieldType type, Object value) {
    checkArgument(
        type.getTypeName().isLogicalType(),
        "convertLogicalTypeFieldToString accepts only logical types");

    Schema.LogicalType<Object, Object> logicalType =
        (Schema.LogicalType<Object, Object>) type.getLogicalType();
    if (logicalType == null) {
      throw new UnsupportedOperationException("Failed to extract logical type");
    }

    Schema.FieldType baseType = logicalType.getBaseType();
    Object baseValue = logicalType.toBaseType(value);
    return convertFieldToString(baseType, baseValue);
  }

  private String convertFieldToString(Schema.FieldType type, Object value) {
    switch (type.getTypeName()) {
      case BYTE:
        return ((Byte) value).toString();
      case INT16:
        return ((Short) value).toString();
      case INT32:
        return ((Integer) value).toString();
      case INT64:
        return ((Long) value).toString();
      case DECIMAL:
        return ((BigDecimal) value).toString();
      case FLOAT:
        return ((Float) value).toString();
      case DOUBLE:
        return ((Double) value).toString();
      case STRING:
        return (String) value;
      case DATETIME:
        return formatter.print((Instant) value);
      case BOOLEAN:
        return ((Boolean) value) ? "1" : "0";
      case BYTES:
        return new String((byte[]) value, StandardCharsets.UTF_8);
      case ARRAY:
        throw new UnsupportedOperationException(
            "Writing of ARRAY type is not supported by the default UserDataMapper");
      case ITERABLE:
        throw new UnsupportedOperationException(
            "Writing of ITERABLE type is not supported by the default UserDataMapper");
      case MAP:
        throw new UnsupportedOperationException(
            "Writing of MAP type is not supported by the default UserDataMapper");
      case ROW:
        throw new UnsupportedOperationException(
            "Writing of nested ROW type is not supported by the default UserDataMapper");
      case LOGICAL_TYPE:
        return convertLogicalTypeFieldToString(type, value);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Writing of %s type is not supported by the default UserDataMapper",
                type.getTypeName().name()));
    }
  }

  @Override
  public List<String> mapRow(Row element) {
    List<String> res = new ArrayList<>();

    Schema s = element.getSchema();
    for (int i = 0; i < s.getFieldCount(); i++) {
      res.add(convertFieldToString(s.getField(i).getType(), element.getValue(i)));
    }

    return res;
  }
}
