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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.spanner.StructUtils.beamRowToStruct;
import static org.apache.beam.sdk.io.gcp.spanner.StructUtils.beamTypeToSpannerType;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
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

  /**
   * Utility function to convert row to mutation for cross-language usage.
   *
   * <p>Spanner mutations are represented as Row with the following schema: table (String): name of
   * the table. operation (String): name of the operation (possible values: {INSERT,
   * INSERT_OR_UPDATE, REPLACE, UPDATE, DELETE}. row (Optional<Row>): row with fields compatible
   * with the mutated table's schema. Used only with INSERT, INSERT_OR_UPDATE, REPLACE, UPDATE
   * operations. keyset (Optional<Array<Row>>): list of rows compatible with the keyset of the
   * mutated table. Used only with DELETE operation.
   *
   * @return function that can convert row to mutation
   */
  public static SerializableFunction<Row, Mutation> beamRowToMutationFn() {
    return (mutationRow -> {
      String table = mutationRow.getString("table");
      String operation = mutationRow.getString("operation");
      checkNotNull(table);
      checkNotNull(operation);
      switch (Mutation.Op.valueOf(operation)) {
        case INSERT:
          checkNotNull(mutationRow.getRow("row"));
          return MutationUtils.createMutationFromBeamRows(
              Mutation.newInsertBuilder(table), mutationRow.getRow("row"));
        case DELETE:
          checkNotNull(mutationRow.getArray("keyset"));
          return Mutation.delete(
              table, MutationUtils.createKeySetFromBeamRows(mutationRow.getArray("keyset")));
        case UPDATE:
          checkNotNull(mutationRow.getRow("row"));
          return MutationUtils.createMutationFromBeamRows(
              Mutation.newUpdateBuilder(table), mutationRow.getRow("row"));
        case REPLACE:
          checkNotNull(mutationRow.getRow("row"));
          return MutationUtils.createMutationFromBeamRows(
              Mutation.newReplaceBuilder(table), mutationRow.getRow("row"));
        case INSERT_OR_UPDATE:
          checkNotNull(mutationRow.getRow("row"));
          return MutationUtils.createMutationFromBeamRows(
              Mutation.newInsertOrUpdateBuilder(table), mutationRow.getRow("row"));
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Unknown mutation operation type: %s", mutationRow.getString("operation")));
      }
    });
  }

  private static KeySet createKeySetFromBeamRows(Iterable<Row> rows) {
    KeySet.Builder builder = KeySet.newBuilder();
    rows.forEach(row -> builder.addKey(createKeyFromBeamRow(row)));
    return builder.build();
  }

  private static Key createKeyFromBeamRow(Row row) {
    Key.Builder builder = Key.newBuilder();
    Schema schema = row.getSchema();
    List<String> columns = schema.getFieldNames();
    columns.forEach(
        columnName ->
            setBeamValueToKey(builder, schema.getField(columnName).getType(), columnName, row));
    return builder.build();
  }

  private static Mutation createMutationFromBeamRows(
      Mutation.WriteBuilder mutationBuilder, Row row) {
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
        keyBuilder.append(row.getByte(columnName).longValue());
        break;
      case INT16:
        keyBuilder.append(row.getInt16(columnName).longValue());
        break;
      case INT32:
        keyBuilder.append(row.getInt32(columnName).longValue());
        break;
      case INT64:
        keyBuilder.append(row.getInt64(columnName));
        break;
      case FLOAT:
        keyBuilder.append(row.getFloat(columnName).doubleValue());
        break;
      case DOUBLE:
        keyBuilder.append(row.getDouble(columnName));
        break;
      case DECIMAL:
        keyBuilder.append(row.getDecimal(columnName));
        break;
        // TODO: Implement logical date and datetime
      case DATETIME:
        keyBuilder.append(
            Timestamp.ofTimeMicroseconds(
                row.getDateTime(columnName).toInstant().getMillis() * 1000L));
        break;
      case BOOLEAN:
        keyBuilder.append(row.getBoolean(columnName));
        break;
      case STRING:
        keyBuilder.append(row.getString(columnName));
        break;
      case BYTES:
        keyBuilder.append(ByteArray.copyFrom(row.getBytes(columnName)));
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported field type: %s", field.getTypeName()));
    }
  }

  private static void setBeamValueToMutation(
      Mutation.WriteBuilder mutationBuilder,
      Schema.FieldType fieldType,
      String columnName,
      Row row) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        mutationBuilder.set(columnName).to(row.getByte(columnName).longValue());
        break;
      case INT16:
        mutationBuilder.set(columnName).to(row.getInt16(columnName).longValue());
        break;
      case INT32:
        mutationBuilder.set(columnName).to(row.getInt32(columnName).longValue());
        break;
      case INT64:
        mutationBuilder.set(columnName).to(row.getInt64(columnName));
        break;
      case FLOAT:
        mutationBuilder.set(columnName).to(row.getFloat(columnName).doubleValue());
        break;
      case DOUBLE:
        mutationBuilder.set(columnName).to(row.getDouble(columnName));
        break;
      case DECIMAL:
        mutationBuilder.set(columnName).to(row.getDecimal(columnName));
        break;
        // TODO: Implement logical date and datetime
      case DATETIME:
        mutationBuilder
            .set(columnName)
            .to(
                Timestamp.ofTimeMicroseconds(
                    row.getDateTime(columnName).toInstant().getMillis() * 1000L));
        break;
      case BOOLEAN:
        mutationBuilder.set(columnName).to(row.getBoolean(columnName));
        break;
      case STRING:
        mutationBuilder.set(columnName).to(row.getString(columnName));
        break;
      case BYTES:
        mutationBuilder.set(columnName).to(ByteArray.copyFrom(row.getBytes(columnName)));
        break;
      case ROW:
        mutationBuilder
            .set(columnName)
            .to(
                beamTypeToSpannerType(row.getSchema().getField(columnName).getType()),
                beamRowToStruct(row.getRow(columnName)));
        break;
      case ARRAY:
        addIterableToMutationBuilder(
            mutationBuilder, row.getArray(columnName), row.getSchema().getField(columnName));
        break;
      case ITERABLE:
        addIterableToMutationBuilder(
            mutationBuilder, row.getIterable(columnName), row.getSchema().getField(columnName));
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported field type: %s", fieldType.getTypeName()));
    }
  }

  @SuppressWarnings("unchecked")
  private static void addIterableToMutationBuilder(
      Mutation.WriteBuilder mutationBuilder, Iterable<Object> iterable, Schema.Field field) {
    String column = field.getName();
    Schema.FieldType beamIterableType = field.getType().getCollectionElementType();
    Schema.TypeName beamIterableTypeName = beamIterableType.getTypeName();
    switch (beamIterableTypeName) {
      case ROW:
        mutationBuilder
            .set(column)
            .toStructArray(
                beamTypeToSpannerType(beamIterableType),
                StreamSupport.stream(iterable.spliterator(), false)
                    .map(row -> beamRowToStruct((Row) row))
                    .collect(toList()));
        break;
      case INT16:
      case INT32:
      case INT64:
      case BYTE:
        mutationBuilder.set(column).toInt64Array((Iterable<Long>) ((Object) iterable));
        break;
      case FLOAT:
      case DOUBLE:
        mutationBuilder.set(column).toFloat64Array((Iterable<Double>) ((Object) iterable));
        break;
      case DECIMAL:
        mutationBuilder.set(column).toNumericArray((Iterable<BigDecimal>) ((Object) iterable));
        break;
      case BOOLEAN:
        mutationBuilder.set(column).toBoolArray((Iterable<Boolean>) ((Object) iterable));
        break;
      case BYTES:
        mutationBuilder
            .set(column)
            .toBytesArray(
                StreamSupport.stream(iterable.spliterator(), false)
                    .map(object -> ByteArray.copyFrom((byte[]) object))
                    .collect(toList()));
        break;
      case STRING:
        mutationBuilder.set(column).toStringArray((Iterable<String>) ((Object) iterable));
        break;
      case DATETIME:
        mutationBuilder
            .set(column)
            .toTimestampArray(
                StreamSupport.stream(iterable.spliterator(), false)
                    .map(datetime -> Timestamp.parseTimestamp((datetime).toString()))
                    .collect(toList()));
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported iterable type while translating row to struct: %s",
                field.getType().getCollectionElementType().getTypeName()));
    }
  }
}
