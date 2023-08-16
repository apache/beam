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

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.joda.time.base.AbstractInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for Calcite related operations. */
public class CalciteUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CalciteUtils.class);

  private static final long UNLIMITED_ARRAY_SIZE = -1L;

  // SQL has schema types that do not directly correspond to Beam Schema types. We define
  // LogicalTypes to represent each of these types.

  /** A LogicalType corresponding to TIME_WITH_LOCAL_TIME_ZONE. */
  public static class TimeWithLocalTzType extends PassThroughLogicalType<Instant> {
    public static final String IDENTIFIER = "SqlTimeWithLocalTzType";

    public TimeWithLocalTzType() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.DATETIME);
    }
  }

  /** Returns true if the type is any of the various date time types. */
  public static boolean isDateTimeType(FieldType fieldType) {
    if (fieldType.getTypeName() == TypeName.DATETIME) {
      return true;
    }

    if (fieldType.getTypeName().isLogicalType()) {
      Schema.LogicalType logicalType = fieldType.getLogicalType();
      Preconditions.checkArgumentNotNull(logicalType);
      String logicalId = logicalType.getIdentifier();
      return logicalId.equals(SqlTypes.DATE.getIdentifier())
          || logicalId.equals(SqlTypes.TIME.getIdentifier())
          || logicalId.equals(TimeWithLocalTzType.IDENTIFIER)
          || logicalId.equals(SqlTypes.DATETIME.getIdentifier());
    }
    return false;
  }

  public static boolean isStringType(FieldType fieldType) {
    if (fieldType.getTypeName() == TypeName.STRING) {
      return true;
    }

    if (fieldType.getTypeName().isLogicalType()) {
      Schema.LogicalType<?, ?> logicalType = fieldType.getLogicalType();
      return logicalType instanceof PassThroughLogicalType
          && logicalType.getBaseType().getTypeName() == TypeName.STRING;
    }
    return false;
  }

  // The list of field type names used in SQL as Beam field types.
  public static final FieldType TINY_INT = FieldType.BYTE;
  public static final FieldType SMALL_INT = FieldType.INT16;
  public static final FieldType INTEGER = FieldType.INT32;
  public static final FieldType BIG_INT = FieldType.INT64;
  public static final FieldType FLOAT = FieldType.FLOAT;
  public static final FieldType DOUBLE = FieldType.DOUBLE;
  public static final FieldType DECIMAL = FieldType.DECIMAL;
  public static final FieldType BOOLEAN = FieldType.BOOLEAN;
  // TODO(https://github.com/apache/beam/issues/24019) Support sql types with arguments
  public static final FieldType VARBINARY = FieldType.BYTES;
  public static final FieldType VARCHAR = FieldType.STRING;
  public static final FieldType CHAR = FieldType.STRING;
  public static final FieldType DATE = FieldType.logicalType(SqlTypes.DATE);
  public static final FieldType NULLABLE_DATE =
      FieldType.logicalType(SqlTypes.DATE).withNullable(true);
  public static final FieldType TIME = FieldType.logicalType(SqlTypes.TIME);
  public static final FieldType NULLABLE_TIME =
      FieldType.logicalType(SqlTypes.TIME).withNullable(true);
  public static final FieldType TIME_WITH_LOCAL_TZ =
      FieldType.logicalType(new TimeWithLocalTzType());
  public static final FieldType TIMESTAMP = FieldType.DATETIME;
  public static final FieldType NULLABLE_TIMESTAMP = FieldType.DATETIME.withNullable(true);
  public static final FieldType TIMESTAMP_WITH_LOCAL_TZ = FieldType.logicalType(SqlTypes.DATETIME);
  public static final FieldType NULLABLE_TIMESTAMP_WITH_LOCAL_TZ =
      FieldType.logicalType(SqlTypes.DATETIME).withNullable(true);

  private static final BiMap<FieldType, SqlTypeName> BEAM_TO_CALCITE_TYPE_MAPPING =
      ImmutableBiMap.<FieldType, SqlTypeName>builder()
          .put(TINY_INT, SqlTypeName.TINYINT)
          .put(SMALL_INT, SqlTypeName.SMALLINT)
          .put(INTEGER, SqlTypeName.INTEGER)
          .put(BIG_INT, SqlTypeName.BIGINT)
          .put(FLOAT, SqlTypeName.FLOAT)
          .put(DOUBLE, SqlTypeName.DOUBLE)
          .put(DECIMAL, SqlTypeName.DECIMAL)
          .put(BOOLEAN, SqlTypeName.BOOLEAN)
          .put(VARBINARY, SqlTypeName.VARBINARY)
          .put(VARCHAR, SqlTypeName.VARCHAR)
          .put(DATE, SqlTypeName.DATE)
          .put(TIME, SqlTypeName.TIME)
          .put(TIME_WITH_LOCAL_TZ, SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
          .put(TIMESTAMP, SqlTypeName.TIMESTAMP)
          .put(TIMESTAMP_WITH_LOCAL_TZ, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .build();

  private static final ImmutableMap<SqlTypeName, FieldType> CALCITE_TO_BEAM_TYPE_MAPPING =
      ImmutableMap.<SqlTypeName, FieldType>builder()
          .put(SqlTypeName.TINYINT, TINY_INT)
          .put(SqlTypeName.SMALLINT, SMALL_INT)
          .put(SqlTypeName.INTEGER, INTEGER)
          .put(SqlTypeName.BIGINT, BIG_INT)
          .put(SqlTypeName.FLOAT, FLOAT)
          .put(SqlTypeName.DOUBLE, DOUBLE)
          .put(SqlTypeName.DECIMAL, DECIMAL)
          .put(SqlTypeName.BOOLEAN, BOOLEAN)
          // TODO(https://github.com/apache/beam/issues/24019) Support sql types with arguments
          // Handle Calcite VARBINARY/BINARY/VARCHAR/CHAR with
          // VariableBinary/FixedBinary/VariableString/FixedString logical types.
          .put(SqlTypeName.VARBINARY, VARBINARY)
          .put(SqlTypeName.BINARY, VARBINARY)
          .put(SqlTypeName.VARCHAR, VARCHAR)
          .put(SqlTypeName.CHAR, CHAR)
          .put(SqlTypeName.DATE, DATE)
          .put(SqlTypeName.TIME, TIME)
          .put(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, TIME_WITH_LOCAL_TZ)
          .put(SqlTypeName.TIMESTAMP, TIMESTAMP)
          .put(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TZ)
          .build();

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
      case ITERABLE:
        return SqlTypeName.ARRAY;
      case MAP:
        return SqlTypeName.MAP;
      default:
        SqlTypeName typeName = BEAM_TO_CALCITE_TYPE_MAPPING.get(type.withNullable(false));
        if (typeName == null) {
          // This will happen e.g. if looking up a STRING type, and metadata isn't set to say which
          // type of SQL string we want. In this case, use the default mapping.
          typeName = BEAM_TO_CALCITE_DEFAULT_MAPPING.get(type);
        }
        if (typeName == null) {
          Schema.LogicalType<?, ?> logicalType = type.getLogicalType();
          if (logicalType != null) {
            if (logicalType instanceof PassThroughLogicalType) {
              // for pass through logical type, just return its base type
              return toSqlTypeName(logicalType.getBaseType());
            } else if ("SqlCharType".equals(logicalType.getIdentifier())) {
              LOG.warn(
                  "SqlCharType is used in Schema. It was removed in Beam 2.44.0 and should be"
                      + " replaced by FixedString logical type.");
              return SqlTypeName.CHAR;
            } else {
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot find a matching Calcite SqlTypeName for Beam logical type: %s",
                      logicalType.getIdentifier()));
            }
          }
          throw new IllegalArgumentException(
              String.format("Cannot find a matching Calcite SqlTypeName for Beam type: %s", type));
        } else {
          return typeName;
        }
    }
  }

  public static FieldType toFieldType(SqlTypeNameSpec sqlTypeName) {
    return toFieldType(
        Preconditions.checkArgumentNotNull(
            SqlTypeName.get(sqlTypeName.getTypeName().getSimple()),
            "Failed to find Calcite type with name '%s'",
            sqlTypeName.getTypeName().getSimple()));
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
        FieldType fieldType = CALCITE_TO_BEAM_TYPE_MAPPING.get(sqlTypeName);
        if (fieldType == null) {
          throw new IllegalArgumentException(
              "Cannot find a matching Beam FieldType for Calcite type: " + sqlTypeName);
        }
        return fieldType;
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
        return FieldType.array(
            toFieldType(
                Preconditions.checkArgumentNotNull(
                    calciteType.getComponentType(),
                    "Encountered MULTISET type with null component type")));
      case MAP:
        return FieldType.map(
            toFieldType(
                Preconditions.checkArgumentNotNull(
                    calciteType.getKeyType(), "Encountered MAP type with null key type.")),
            toFieldType(
                Preconditions.checkArgumentNotNull(
                    calciteType.getValueType(), "Encountered MAP type with null value type.")));
      case ROW:
        return FieldType.row(toSchema(calciteType));

      default:
        try {
          return toFieldType(calciteType.getSqlTypeName()).withNullable(calciteType.isNullable());
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "Cannot find a matching Beam FieldType for Calcite type: " + calciteType, e);
        }
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

  public static RelDataType toRelDataType(RelDataTypeFactory dataTypeFactory, FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        FieldType collectionElementType = fieldType.getCollectionElementType();
        Preconditions.checkArgumentNotNull(collectionElementType);
        return dataTypeFactory.createArrayType(
            toRelDataType(dataTypeFactory, collectionElementType), UNLIMITED_ARRAY_SIZE);
      case MAP:
        FieldType mapKeyType = fieldType.getMapKeyType();
        FieldType mapValueType = fieldType.getMapValueType();
        Preconditions.checkArgumentNotNull(mapKeyType);
        Preconditions.checkArgumentNotNull(mapValueType);
        RelDataType componentKeyType = toRelDataType(dataTypeFactory, mapKeyType);
        RelDataType componentValueType = toRelDataType(dataTypeFactory, mapValueType);
        return dataTypeFactory.createMapType(componentKeyType, componentValueType);
      case ROW:
        Schema schema = fieldType.getRowSchema();
        Preconditions.checkArgumentNotNull(schema);
        return toCalciteRowType(schema, dataTypeFactory);
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
   * 1. redirect {@link AbstractInstant} to {@link Date} so Calcite can recognize it. <br>
   * 2. For a list, the component type is needed to create a Sql array type. <br>
   * 3. For a Map, the component type is needed to create a Sql map type.
   *
   * @param type
   * @return Calcite RelDataType
   */
  public static RelDataType sqlTypeWithAutoCast(RelDataTypeFactory typeFactory, Type type) {
    // For Joda time types, return SQL type for java.util.Date.
    if (type instanceof Class && AbstractInstant.class.isAssignableFrom((Class<?>) type)) {
      return typeFactory.createJavaType(Date.class);
    } else if (type instanceof Class && ByteString.class.isAssignableFrom((Class<?>) type)) {
      return typeFactory.createJavaType(byte[].class);
    } else if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      if (java.util.List.class.isAssignableFrom((Class<?>) parameterizedType.getRawType())) {
        RelDataType elementType =
            sqlTypeWithAutoCast(typeFactory, parameterizedType.getActualTypeArguments()[0]);
        return typeFactory.createArrayType(elementType, UNLIMITED_ARRAY_SIZE);
      } else if (java.util.Map.class.isAssignableFrom((Class<?>) parameterizedType.getRawType())) {
        RelDataType mapElementKeyType =
            sqlTypeWithAutoCast(typeFactory, parameterizedType.getActualTypeArguments()[0]);
        RelDataType mapElementValueType =
            sqlTypeWithAutoCast(typeFactory, parameterizedType.getActualTypeArguments()[1]);
        return typeFactory.createMapType(mapElementKeyType, mapElementValueType);
      }
    } else if (type instanceof GenericArrayType) {
      throw new IllegalArgumentException(
          "Cannot infer types from "
              + type
              + ". This is currently unsupported, use List instead "
              + "of Array.");
    }
    return typeFactory.createJavaType((Class) type);
  }
}
