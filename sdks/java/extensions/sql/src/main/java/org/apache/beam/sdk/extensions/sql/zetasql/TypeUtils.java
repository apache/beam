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
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_DOUBLE;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_FLOAT;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_INT32;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;

/** Utility to convert types from Calcite Schema types. */
@Internal
public class TypeUtils {

  private static final ImmutableMap<SqlTypeName, Type> CALCITE_TO_ZETA_SIMPLE_TYPES =
      ImmutableMap.<SqlTypeName, Type>builder()
          .put(SqlTypeName.BIGINT, TypeFactory.createSimpleType(TYPE_INT64))
          .put(SqlTypeName.INTEGER, TypeFactory.createSimpleType(TYPE_INT32))
          .put(SqlTypeName.VARCHAR, TypeFactory.createSimpleType(TYPE_STRING))
          .put(SqlTypeName.BOOLEAN, TypeFactory.createSimpleType(TYPE_BOOL))
          .put(SqlTypeName.FLOAT, TypeFactory.createSimpleType(TYPE_FLOAT))
          .put(SqlTypeName.DOUBLE, TypeFactory.createSimpleType(TYPE_DOUBLE))
          .put(SqlTypeName.VARBINARY, TypeFactory.createSimpleType(TYPE_BYTES))
          .put(SqlTypeName.TIMESTAMP, TypeFactory.createSimpleType(TYPE_TIMESTAMP))
          .put(SqlTypeName.DATE, TypeFactory.createSimpleType(TYPE_DATE))
          .put(SqlTypeName.TIME, TypeFactory.createSimpleType(TYPE_TIME))
          .build();

  private static final ImmutableMap<TypeKind, Function<RexBuilder, RelDataType>>
      ZETA_TO_CALCITE_SIMPLE_TYPES =
          ImmutableMap.<TypeKind, Function<RexBuilder, RelDataType>>builder()
              .put(TYPE_NUMERIC, relDataTypeFactory(SqlTypeName.DECIMAL))
              .put(TYPE_INT32, relDataTypeFactory(SqlTypeName.INTEGER))
              .put(TYPE_INT64, relDataTypeFactory(SqlTypeName.BIGINT))
              .put(TYPE_FLOAT, relDataTypeFactory(SqlTypeName.FLOAT))
              .put(TYPE_DOUBLE, relDataTypeFactory(SqlTypeName.DOUBLE))
              .put(TYPE_STRING, relDataTypeFactory(SqlTypeName.VARCHAR))
              .put(TYPE_BOOL, relDataTypeFactory(SqlTypeName.BOOLEAN))
              .put(TYPE_BYTES, relDataTypeFactory(SqlTypeName.VARBINARY))
              .put(TYPE_DATE, relDataTypeFactory(SqlTypeName.DATE))
              .put(TYPE_TIME, relDataTypeFactory(SqlTypeName.TIME))
              // TODO: handle timestamp with time zone.
              .put(TYPE_TIMESTAMP, relDataTypeFactory(SqlTypeName.TIMESTAMP))
              .build();

  /** Returns a type matching the corresponding Calcite type. */
  static Type toZetaType(RelDataType calciteType) {

    if (CALCITE_TO_ZETA_SIMPLE_TYPES.containsKey(calciteType.getSqlTypeName())) {
      return CALCITE_TO_ZETA_SIMPLE_TYPES.get(calciteType.getSqlTypeName());
    }

    switch (calciteType.getSqlTypeName()) {
      case ARRAY:
        return TypeFactory.createArrayType(toZetaType(calciteType.getComponentType()));
      case MAP:

        // it is ok to return a simple type for MAP because MAP only exists in pubsub table which
        // used to save table attribute.
        // TODO: have a better way to handle MAP given the fact that ZetaSQL has no MAP type.
        return TypeFactory.createSimpleType(TypeKind.TYPE_STRING);
      case ROW:
        List<StructField> structFields =
            calciteType.getFieldList().stream()
                .map(f -> new StructField(f.getName(), toZetaType(f.getType())))
                .collect(toList());

        return TypeFactory.createStructType(structFields);
      default:
        throw new RuntimeException("Unsupported RelDataType: " + calciteType);
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
    return rexBuilder
        .getTypeFactory()
        .createArrayType(toRelDataType(rexBuilder, arrayType.getElementType(), isNullable), -1);
  }

  private static RelDataType toStructRelDataType(
      RexBuilder rexBuilder, StructType structType, boolean isNullable) {

    List<StructField> fields = structType.getFieldList();
    List<String> fieldNames = fields.stream().map(StructField::getName).collect(toList());
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
    if (!ZETA_TO_CALCITE_SIMPLE_TYPES.containsKey(kind)) {
      throw new RuntimeException("Unsupported column type: " + kind);
    }

    RelDataType relDataType = ZETA_TO_CALCITE_SIMPLE_TYPES.get(kind).apply(rexBuilder);
    return nullable(rexBuilder, relDataType, isNullable);
  }

  private static RelDataType nullable(RexBuilder r, RelDataType relDataType, boolean isNullable) {
    return r.getTypeFactory().createTypeWithNullability(relDataType, isNullable);
  }

  private static Function<RexBuilder, RelDataType> relDataTypeFactory(SqlTypeName typeName) {
    return (RexBuilder r) -> r.getTypeFactory().createSqlType(typeName);
  }
}
