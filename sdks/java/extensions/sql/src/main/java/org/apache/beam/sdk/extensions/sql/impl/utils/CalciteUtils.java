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
package org.apache.beam.sdk.extensions.sql.impl.utils;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.base.AbstractInstant;

/** Utility methods for Calcite related operations. */
public class CalciteUtils {
  private static final long UNLIMITED_ARRAY_SIZE = -1L;
  // Beam's Schema class has a single DATETIME type, so we need a way to distinguish the different
  // Calcite time classes. We do this by storing extra metadata in the FieldType so we
  // can tell which time class this is.
  //
  // Same story with CHAR and VARCHAR - they both map to STRING.
  private static final BiMap<FieldType, SqlTypeName> BEAM_TO_CALCITE_TYPE_MAPPING =
      ImmutableBiMap.<FieldType, SqlTypeName>builder()
          .put(FieldType.BYTE, SqlTypeName.TINYINT)
          .put(FieldType.INT16, SqlTypeName.SMALLINT)
          .put(FieldType.INT32, SqlTypeName.INTEGER)
          .put(FieldType.INT64, SqlTypeName.BIGINT)
          .put(FieldType.FLOAT, SqlTypeName.FLOAT)
          .put(FieldType.DOUBLE, SqlTypeName.DOUBLE)
          .put(FieldType.DECIMAL, SqlTypeName.DECIMAL)
          .put(FieldType.BOOLEAN, SqlTypeName.BOOLEAN)
          .put(FieldType.BYTES, SqlTypeName.VARBINARY)
          .put(FieldType.DATETIME.withMetadata("DATE"), SqlTypeName.DATE)
          .put(FieldType.DATETIME.withMetadata("TIME"), SqlTypeName.TIME)
          .put(
              FieldType.DATETIME.withMetadata("TIME_WITH_LOCAL_TZ"),
              SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
          .put(FieldType.DATETIME, SqlTypeName.TIMESTAMP)
          .put(
              FieldType.DATETIME.withMetadata("TS_WITH_LOCAL_TZ"),
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .put(FieldType.STRING.withMetadata("CHAR"), SqlTypeName.CHAR)
          .put(FieldType.STRING, SqlTypeName.VARCHAR)
          .build();

  private static final BiMap<SqlTypeName, FieldType> CALCITE_TO_BEAM_TYPE_MAPPING =
      BEAM_TO_CALCITE_TYPE_MAPPING.inverse();

  // The list of field type names used in SQL as Beam field types.
  public static final FieldType TINY_INT = toFieldType(SqlTypeName.TINYINT);
  public static final FieldType SMALL_INT = toFieldType(SqlTypeName.SMALLINT);
  public static final FieldType INTEGER = toFieldType(SqlTypeName.INTEGER);
  public static final FieldType BIG_INT = toFieldType(SqlTypeName.BIGINT);
  public static final FieldType FLOAT = toFieldType(SqlTypeName.FLOAT);
  public static final FieldType DOUBLE = toFieldType(SqlTypeName.DOUBLE);
  public static final FieldType DECIMAL = toFieldType(SqlTypeName.DECIMAL);
  public static final FieldType BOOLEAN = toFieldType(SqlTypeName.BOOLEAN);
  public static final FieldType CHAR = toFieldType(SqlTypeName.CHAR);
  public static final FieldType VARCHAR = toFieldType(SqlTypeName.VARCHAR);
  public static final FieldType TIME = toFieldType(SqlTypeName.TIME);
  public static final FieldType DATE = toFieldType(SqlTypeName.DATE);
  public static final FieldType TIMESTAMP = toFieldType(SqlTypeName.TIMESTAMP);

  // Since there are multiple Calcite type that correspond to a single Beam type, this is the
  // default mapping.
  private static final Map<FieldType, SqlTypeName> BEAM_TO_CALCITE_DEFAULT_MAPPING =
      ImmutableMap.of(
          FieldType.DATETIME, SqlTypeName.TIMESTAMP,
          FieldType.STRING, SqlTypeName.VARCHAR);

  /** Generate {@link Schema} from {@code RelDataType} which is used to create table. */
  public static Schema toSchema(RelDataType tableInfo) {
    return tableInfo.getFieldList().stream().map(CalciteUtils::toField).collect(Schema.toSchema());
  }

  public static SqlTypeName toSqlTypeName(FieldType type) {
    switch (type.getTypeName()) {
      case ROW:
        return SqlTypeName.ROW;
      case ARRAY:
        return SqlTypeName.ARRAY;
      case MAP:
        return SqlTypeName.MAP;
      default:
        SqlTypeName typeName = BEAM_TO_CALCITE_TYPE_MAPPING.get(type.withNullable(false));
        if (typeName != null) {
          return typeName;
        } else {
          // This will happen e.g. if looking up a STRING type, and metadata isn't set to say which
          // type of SQL string we want. In this case, use the default mapping.
          return BEAM_TO_CALCITE_DEFAULT_MAPPING.get(type.withNullable(false));
        }
    }
  }

  public static FieldType toFieldType(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
      case MAP:
      case MULTISET:
      case ARRAY:
      case ROW:
        throw new IllegalArgumentException(
            String.format(
                "%s is a type constructor that takes parameters, not a type,"
                    + "so it cannot be converted to a %s",
                sqlTypeName, Schema.FieldType.class.getSimpleName()));
      default:
        return CALCITE_TO_BEAM_TYPE_MAPPING.get(sqlTypeName).withNullable(true);
    }
  }

  public static Schema.Field toField(RelDataTypeField calciteField) {
    return toField(calciteField.getName(), calciteField.getType());
  }

  public static Schema.Field toField(String name, RelDataType calciteType) {
    return Schema.Field.of(name, toFieldType(calciteType)).withNullable(calciteType.isNullable());
  }

  public static FieldType toFieldType(RelDataType calciteType) {
    switch (calciteType.getSqlTypeName()) {
      case ARRAY:
      case MULTISET:
        return FieldType.array(toFieldType(calciteType.getComponentType()));
      case MAP:
        return FieldType.map(
            toFieldType(calciteType.getKeyType()), toFieldType(calciteType.getValueType()));
      case ROW:
        return FieldType.row(toSchema(calciteType));

      default:
        return toFieldType(calciteType.getSqlTypeName());
    }
  }

  /** Create an instance of {@code RelDataType} so it can be used to create a table. */
  public static RelDataType toCalciteRowType(Schema schema, RelDataTypeFactory dataTypeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(dataTypeFactory);

    IntStream.range(0, schema.getFieldCount())
        .forEach(
            idx ->
                builder.add(
                    schema.getField(idx).getName(), toRelDataType(dataTypeFactory, schema, idx)));
    return builder.build();
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory, FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ARRAY:
        return dataTypeFactory.createArrayType(
            toRelDataType(dataTypeFactory, fieldType.getCollectionElementType()),
            UNLIMITED_ARRAY_SIZE);
      case MAP:
        RelDataType componentKeyType = toRelDataType(dataTypeFactory, fieldType.getMapKeyType());
        RelDataType componentValueType =
            toRelDataType(dataTypeFactory, fieldType.getMapValueType());
        return dataTypeFactory.createMapType(componentKeyType, componentValueType);
      case ROW:
        return toCalciteRowType(fieldType.getRowSchema(), dataTypeFactory);
      default:
        return dataTypeFactory.createSqlType(toSqlTypeName(fieldType));
    }
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory, Schema schema, int fieldIndex) {
    Schema.Field field = schema.getField(fieldIndex);
    RelDataType type = toRelDataType(dataTypeFactory, field.getType());

    return dataTypeFactory.createTypeWithNullability(type, field.getType().getNullable());
  }

  /**
   * SQL-Java type mapping, with specified Beam rules: <br>
   * 1. redirect {@link AbstractInstant} to {@link Date} so Calcite can recognize it.
   *
   * @param rawType
   * @return
   */
  public static RelDataType sqlTypeWithAutoCast(RelDataTypeFactory typeFactory, Type rawType) {
    //For Joda time types, return SQL type for java.util.Date.
    if (rawType instanceof Class && AbstractInstant.class.isAssignableFrom((Class<?>) rawType)) {
      return typeFactory.createJavaType(Date.class);
    } else if (rawType instanceof Class && ByteString.class.isAssignableFrom((Class<?>) rawType)) {
      return typeFactory.createJavaType(byte[].class);
    }
    return typeFactory.createJavaType((Class) rawType);
  }
}
