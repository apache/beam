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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ValueBinder;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

final class MutationUtils {
  private MutationUtils() {}

  /**
   * Check if the mutation is a delete by a single primary key operation.
   *
   * @param m mutation
   * @return true if mutation is a point delete
   */
  public static boolean isPointDelete(Mutation m) {
    return m.getOperation() == Mutation.Op.DELETE
        && Iterables.isEmpty(m.getKeySet().getRanges())
        && Iterables.size(m.getKeySet().getKeys()) == 1;
  }

  public static KeySet createKeySetFromBeamRows(Iterable<Row> rows) {
    KeySet.Builder builder = KeySet.newBuilder();
    rows.forEach(MutationUtils::createKeyFromBeamRow);
    return builder.build();
  }

  public static Key createKeyFromBeamRow(Row row) {
    Key.Builder builder = Key.newBuilder();
    Schema schema = row.getSchema();
    List<String> columns = schema.getFieldNames();
    columns.forEach(
        columnName ->
            setBeamValueToKey(builder, schema.getField(columnName).getType(), columnName, row));
    return builder.build();
  }

  public static Mutation createMutationFromBeamRow(Mutation.WriteBuilder mutationBuilder, Row row) {
    Schema schema = row.getSchema();
    List<String> columns = schema.getFieldNames();
    columns.forEach(
        columnName ->
            setBeamValueToMutation(
                mutationBuilder, schema.getField(columnName).getType(), columnName, row));
    return mutationBuilder.build();
  }

  private static void setBeamValueToKey(
      Key.Builder keyBuilder, Schema.FieldType field, String columnName, Row row) {
    switch (field.getTypeName()) {
      case BYTE:
        keyBuilder.append(row.getByte(columnName));
        break;
      case INT16:
        keyBuilder.append(row.getInt16(columnName));
        break;
      case INT32:
        keyBuilder.append(row.getInt32(columnName));
        break;
      case INT64:
        keyBuilder.append(row.getInt64(columnName));
        break;
      case FLOAT:
        keyBuilder.append(row.getFloat(columnName));
        break;
      case DOUBLE:
        keyBuilder.append(row.getDouble(columnName));
        break;
      case DATETIME:
        keyBuilder.append(
            Timestamp.ofTimeMicroseconds(row.getDateTime(columnName).toInstant().getMillis()));
        break;
      case BOOLEAN:
        keyBuilder.append(row.getBoolean(columnName));
        break;
      case STRING:
        keyBuilder.append(row.getString(columnName));
        break;
      case DECIMAL:
      default:
        throw new RuntimeException(
            String.format("Unsupported field type: %s", field.getTypeName()));
    }
  }

  private static void setBeamValueToMutation(
      Mutation.WriteBuilder builder, Schema.FieldType fieldType, String columnName, Row row) {
    ValueBinder<Mutation.WriteBuilder> binder = builder.set(columnName);
    switch (fieldType.getTypeName()) {
      case BYTE:
        binder.to(row.getByte(columnName));
        break;
      case INT16:
        binder.to(row.getInt16(columnName));
        break;
      case INT32:
        binder.to(row.getInt32(columnName));
        break;
      case INT64:
        binder.to(row.getInt64(columnName));
        break;
      case FLOAT:
        binder.to(row.getFloat(columnName));
        break;
      case DOUBLE:
        binder.to(row.getDouble(columnName));
        break;
      case DATETIME:
        binder.to(
            Timestamp.ofTimeMicroseconds(row.getDateTime(columnName).toInstant().getMillis()));
        break;
      case BOOLEAN:
        binder.to(row.getBoolean(columnName));
        break;
      case STRING:
        binder.to(row.getString(columnName));
        break;
      case DECIMAL:
      default:
        throw new RuntimeException(
            String.format("Unsupported field type: %s", fieldType.getTypeName()));
    }
  }
}
