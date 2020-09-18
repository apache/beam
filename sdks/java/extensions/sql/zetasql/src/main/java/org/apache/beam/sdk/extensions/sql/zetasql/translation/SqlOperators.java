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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.extensions.sql.impl.udaf.StringAgg;
import org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.impl.BeamBuiltinMethods;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.impl.CastFunctionImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.AggregateFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.ScalarFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.InferTypes;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.OperandTypes;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Optionality;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/**
 * A separate SqlOperators table for those functions that do not exist or not compatible with
 * Calcite. Most of functions within this class is copied from Calcite.
 */
@Internal
public class SqlOperators {
  public static final SqlOperator ZETASQL_TIMESTAMP_ADD =
      createZetaSqlFunction("timestamp_add", SqlTypeName.TIMESTAMP);

  private static final RelDataType OTHER = createSqlType(SqlTypeName.OTHER, false);
  private static final RelDataType TIMESTAMP = createSqlType(SqlTypeName.TIMESTAMP, false);
  private static final RelDataType NULLABLE_TIMESTAMP = createSqlType(SqlTypeName.TIMESTAMP, true);
  private static final RelDataType BIGINT = createSqlType(SqlTypeName.BIGINT, false);
  private static final RelDataType NULLABLE_BIGINT = createSqlType(SqlTypeName.BIGINT, true);

  public static final SqlOperator STRING_AGG_STRING_FN =
      createUdafOperator(
          "string_agg",
          x -> createTypeFactory().createSqlType(SqlTypeName.VARCHAR),
          new UdafImpl<>(new StringAgg.StringAggString()));

  public static final SqlOperator START_WITHS =
      createUdfOperator("STARTS_WITH", BeamBuiltinMethods.STARTS_WITH_METHOD);

  public static final SqlOperator CONCAT =
      createUdfOperator("CONCAT", BeamBuiltinMethods.CONCAT_METHOD);

  public static final SqlOperator REPLACE =
      createUdfOperator("REPLACE", BeamBuiltinMethods.REPLACE_METHOD);

  public static final SqlOperator TRIM = createUdfOperator("TRIM", BeamBuiltinMethods.TRIM_METHOD);

  public static final SqlOperator LTRIM =
      createUdfOperator("LTRIM", BeamBuiltinMethods.LTRIM_METHOD);

  public static final SqlOperator RTRIM =
      createUdfOperator("RTRIM", BeamBuiltinMethods.RTRIM_METHOD);

  public static final SqlOperator SUBSTR =
      createUdfOperator("SUBSTR", BeamBuiltinMethods.SUBSTR_METHOD);

  public static final SqlOperator REVERSE =
      createUdfOperator("REVERSE", BeamBuiltinMethods.REVERSE_METHOD);

  public static final SqlOperator CHAR_LENGTH =
      createUdfOperator("CHAR_LENGTH", BeamBuiltinMethods.CHAR_LENGTH_METHOD);

  public static final SqlOperator ENDS_WITH =
      createUdfOperator("ENDS_WITH", BeamBuiltinMethods.ENDS_WITH_METHOD);

  public static final SqlOperator LIKE =
      createUdfOperator("LIKE", BeamBuiltinMethods.LIKE_METHOD, SqlSyntax.BINARY);

  public static final SqlOperator VALIDATE_TIMESTAMP =
      createUdfOperator(
          "validateTimestamp",
          DateTimeUtils.class,
          "validateTimestamp",
          x -> NULLABLE_TIMESTAMP,
          ImmutableList.of(TIMESTAMP));

  public static final SqlOperator VALIDATE_TIME_INTERVAL =
      createUdfOperator(
          "validateIntervalArgument",
          DateTimeUtils.class,
          "validateTimeInterval",
          x -> NULLABLE_BIGINT,
          ImmutableList.of(BIGINT, OTHER));

  public static final SqlOperator TIMESTAMP_OP =
      createUdfOperator("TIMESTAMP", BeamBuiltinMethods.TIMESTAMP_METHOD);

  public static final SqlOperator DATE_OP =
      createUdfOperator("DATE", BeamBuiltinMethods.DATE_METHOD);

  @SuppressWarnings("nullness")
  public static final SqlUserDefinedFunction CAST_OP =
      new SqlUserDefinedFunction(
          new SqlIdentifier("CAST", SqlParserPos.ZERO),
          null,
          null,
          null,
          null,
          new CastFunctionImpl());

  /**
   * Create a dummy SqlFunction of type OTHER_FUNCTION from given function name and return type.
   * These functions will be unparsed in BeamZetaSqlCalcRel and then executed by ZetaSQL evaluator.
   */
  @SuppressWarnings("nullness")
  public static SqlFunction createZetaSqlFunction(String name, SqlTypeName returnType) {
    return new SqlFunction(
        name,
        SqlKind.OTHER_FUNCTION,
        x -> createSqlType(returnType, true),
        null, // operandTypeInference
        null, // operandTypeChecker
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @SuppressWarnings("nullness")
  private static SqlUserDefinedAggFunction createUdafOperator(
      String name, SqlReturnTypeInference returnTypeInference, AggregateFunction function) {
    return new SqlUserDefinedAggFunction(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        returnTypeInference,
        null,
        null,
        function,
        false,
        false,
        Optionality.FORBIDDEN,
        createTypeFactory());
  }

  @SuppressWarnings("nullness")
  private static SqlUserDefinedFunction createUdfOperator(
      String name,
      Class<?> methodClass,
      String methodName,
      SqlReturnTypeInference returnTypeInference,
      List<RelDataType> paramTypes) {
    return new SqlUserDefinedFunction(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        returnTypeInference,
        null,
        null,
        paramTypes,
        ScalarFunctionImpl.create(methodClass, methodName));
  }

  // Helper function to create SqlUserDefinedFunction based on a function name and a method.
  // SqlUserDefinedFunction will be able to pass through Calcite codegen and get proper function
  // called.
  private static SqlUserDefinedFunction createUdfOperator(String name, Method method) {
    return createUdfOperator(name, method, SqlSyntax.FUNCTION);
  }

  private static SqlUserDefinedFunction createUdfOperator(
      String name, Method method, final SqlSyntax syntax) {
    Function function = ScalarFunctionImpl.create(method);
    final RelDataTypeFactory typeFactory = createTypeFactory();

    List<RelDataType> argTypes = new ArrayList<>();
    List<SqlTypeFamily> typeFamilies = new ArrayList<>();
    for (FunctionParameter o : function.getParameters()) {
      final RelDataType type = o.getType(typeFactory);
      argTypes.add(type);
      typeFamilies.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }

    final FamilyOperandTypeChecker typeChecker =
        OperandTypes.family(typeFamilies, i -> function.getParameters().get(i).isOptional());

    final List<RelDataType> paramTypes = toSql(typeFactory, argTypes);

    return new SqlUserDefinedFunction(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        infer((ScalarFunction) function),
        InferTypes.explicit(argTypes),
        typeChecker,
        paramTypes,
        function) {
      @Override
      public SqlSyntax getSyntax() {
        return syntax;
      }
    };
  }

  private static RelDataType createSqlType(SqlTypeName typeName, boolean withNullability) {
    final RelDataTypeFactory typeFactory = createTypeFactory();
    RelDataType type = typeFactory.createSqlType(typeName);
    if (withNullability) {
      type = typeFactory.createTypeWithNullability(type, true);
    }
    return type;
  }

  private static RelDataTypeFactory createTypeFactory() {
    return new SqlTypeFactoryImpl(BeamRelDataTypeSystem.INSTANCE);
  }

  private static SqlReturnTypeInference infer(final ScalarFunction function) {
    return opBinding -> {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType type;
      if (function instanceof ScalarFunctionImpl) {
        type = ((ScalarFunctionImpl) function).getReturnType(typeFactory, opBinding);
      } else {
        type = function.getReturnType(typeFactory);
      }
      return toSql(typeFactory, type);
    };
  }

  private static List<RelDataType> toSql(
      final RelDataTypeFactory typeFactory, List<RelDataType> types) {
    return Lists.transform(types, type -> toSql(typeFactory, checkArgumentNotNull(type)));
  }

  private static RelDataType toSql(RelDataTypeFactory typeFactory, RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType
        && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass() == Object.class) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    }
    return JavaTypeFactoryImpl.toSql(typeFactory, type);
  }
}
