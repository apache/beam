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
package org.apache.beam.sdk.extensions.sql.zetasql;

import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_BOOL;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_BYTES;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_DATE;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_DATETIME;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_DOUBLE;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_INT64;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_NUMERIC;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_STRING;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_TIME;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_TIMESTAMP;
import static java.util.stream.Collectors.toList;

import com.google.zetasql.ArrayType;
import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Utility methods for ZetaSQL <=> Calcite translation.
 *
 * <p>Unsupported ZetaSQL types: INT32, UINT32, UINT64, FLOAT, ENUM, PROTO, GEOGRAPHY
 */
@Internal
public final class ZetaSqlCalciteTranslationUtils {

  private ZetaSqlCalciteTranslationUtils() {}

  // Type conversion: Calcite => ZetaSQL
  public static Type toZetaType(RelDataType calciteType) {
    switch (calciteType.getSqlTypeName()) {
      case BIGINT:
        return TypeFactory.createSimpleType(TYPE_INT64);
      case DOUBLE:
        return TypeFactory.createSimpleType(TYPE_DOUBLE);
      case BOOLEAN:
        return TypeFactory.createSimpleType(TYPE_BOOL);
      case VARCHAR:
        return TypeFactory.createSimpleType(TYPE_STRING);
      case VARBINARY:
        return TypeFactory.createSimpleType(TYPE_BYTES);
      case DECIMAL:
        return TypeFactory.createSimpleType(TYPE_NUMERIC);
      case DATE:
        return TypeFactory.createSimpleType(TYPE_DATE);
      case TIME:
        return TypeFactory.createSimpleType(TYPE_TIME);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TypeFactory.createSimpleType(TYPE_DATETIME);
      case TIMESTAMP:
        return TypeFactory.createSimpleType(TYPE_TIMESTAMP);
      case ARRAY:
        return TypeFactory.createArrayType(toZetaType(calciteType.getComponentType()));
      case ROW:
        List<StructField> structFields =
            calciteType.getFieldList().stream()
                .map(f -> new StructField(f.getName(), toZetaType(f.getType())))
                .collect(toList());

        return TypeFactory.createStructType(structFields);
      default:
        throw new UnsupportedOperationException("Unsupported RelDataType: " + calciteType);
    }
  }

  // Type conversion: ZetaSQL => Calcite
  public static SqlTypeName toCalciteTypeName(TypeKind type) {
    switch (type) {
      case TYPE_INT64:
        return SqlTypeName.BIGINT;
      case TYPE_DOUBLE:
        return SqlTypeName.DOUBLE;
      case TYPE_BOOL:
        return SqlTypeName.BOOLEAN;
      case TYPE_STRING:
        return SqlTypeName.VARCHAR;
      case TYPE_BYTES:
        return SqlTypeName.VARBINARY;
      case TYPE_NUMERIC:
        return SqlTypeName.DECIMAL;
      case TYPE_DATE:
        return SqlTypeName.DATE;
      case TYPE_TIME:
        return SqlTypeName.TIME;
      case TYPE_DATETIME:
        return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
      case TYPE_TIMESTAMP:
        // TODO: handle timestamp with time zone.
        return SqlTypeName.TIMESTAMP;
        // TODO[BEAM-9179] Add conversion code for ARRAY and ROW types
      default:
        throw new UnsupportedOperationException("Unknown ZetaSQL type: " + type.name());
    }
  }

  public static RelDataType toRelDataType(RexBuilder rexBuilder, Type type, boolean isNullable) {
    if (type.getKind().equals(TypeKind.TYPE_ARRAY)) {
      return toArrayRelDataType(rexBuilder, type.asArray(), isNullable);
    } else if (type.getKind().equals(TypeKind.TYPE_STRUCT)) {
      return toStructRelDataType(rexBuilder, type.asStruct(), isNullable);
    } else {
      // TODO: Check type's nullability?
      return toSimpleRelDataType(type.getKind(), rexBuilder, isNullable);
    }
  }

  public static RelDataType toArrayRelDataType(
      RexBuilder rexBuilder, ArrayType arrayType, boolean isNullable) {
    // -1 cardinality means unlimited array size.
    // TODO: is unlimited array size right for general case?
    // TODO: whether isNullable should be ArrayType's nullablity (not its element type's?)
    return nullable(
        rexBuilder,
        rexBuilder
            .getTypeFactory()
            .createArrayType(toRelDataType(rexBuilder, arrayType.getElementType(), isNullable), -1),
        isNullable);
  }

  private static List<String> toNameList(List<StructField> fields) {
    ImmutableList.Builder<String> b = ImmutableList.builder();
    for (int i = 0; i < fields.size(); i++) {
      String name = fields.get(i).getName();
      if ("".equals(name)) {
        name = "$col" + i;
      }
      b.add(name);
    }
    return b.build();
  }

  public static RelDataType toStructRelDataType(
      RexBuilder rexBuilder, StructType structType, boolean isNullable) {

    List<StructField> fields = structType.getFieldList();
    List<String> fieldNames = toNameList(fields);
    List<RelDataType> fieldTypes =
        fields.stream()
            .map(f -> toRelDataType(rexBuilder, f.getType(), isNullable))
            .collect(toList());

    return rexBuilder.getTypeFactory().createStructType(fieldTypes, fieldNames);
  }

  // TODO: convert TIMESTAMP with/without TIMEZONE and DATETIME.
  public static RelDataType toSimpleRelDataType(TypeKind kind, RexBuilder rexBuilder) {
    return toSimpleRelDataType(kind, rexBuilder, true);
  }

  public static RelDataType toSimpleRelDataType(
      TypeKind kind, RexBuilder rexBuilder, boolean isNullable) {
    RelDataType relDataType = relDataTypeFactory(toCalciteTypeName(kind)).apply(rexBuilder);
    return nullable(rexBuilder, relDataType, isNullable);
  }

  private static RelDataType nullable(RexBuilder r, RelDataType relDataType, boolean isNullable) {
    return r.getTypeFactory().createTypeWithNullability(relDataType, isNullable);
  }

  private static Function<RexBuilder, RelDataType> relDataTypeFactory(SqlTypeName typeName) {
    return (RexBuilder r) -> r.getTypeFactory().createSqlType(typeName);
  }
}
