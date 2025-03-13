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

import static org.apache.beam.sdk.extensions.sql.zetasql.BeamZetaSqlCatalog.ZETASQL_FUNCTION_GROUP_NAME;

import com.google.zetasql.Value;
import com.google.zetasql.io.grpc.Status;
import com.google.zetasql.io.grpc.StatusRuntimeException;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAggregations;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.CountIf;
import org.apache.beam.sdk.extensions.sql.impl.udaf.ArrayAgg;
import org.apache.beam.sdk.extensions.sql.impl.udaf.StringAgg;
import org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlException;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.impl.BeamBuiltinMethods;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.impl.CastFunctionImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.AggregateFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.ScalarFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.ArraySqlType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.InferTypes;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.OperandTypes;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Optionality;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/**
 * A separate SqlOperators table for those functions that do not exist or not compatible with
 * Calcite. Most of functions within this class is copied from Calcite.
 */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SqlOperators {
  public static final SqlOperator ZETASQL_TIMESTAMP_ADD =
      createZetaSqlFunction("timestamp_add", SqlTypeName.TIMESTAMP);

  private static final RelDataType OTHER = createSqlType(SqlTypeName.OTHER, false);
  private static final RelDataType TIMESTAMP = createSqlType(SqlTypeName.TIMESTAMP, false);
  private static final RelDataType NULLABLE_TIMESTAMP = createSqlType(SqlTypeName.TIMESTAMP, true);
  private static final RelDataType BIGINT = createSqlType(SqlTypeName.BIGINT, false);
  private static final RelDataType NULLABLE_BIGINT = createSqlType(SqlTypeName.BIGINT, true);

  public static final SqlOperator ARRAY_AGG_FN =
      createUdafOperator(
          "array_agg",
          x -> new ArraySqlType(x.getOperandType(0), true),
          new UdafImpl<>(new ArrayAgg.ArrayAggArray<>()));

  public static final SqlOperator START_WITHS =
      createUdfOperator(
          "STARTS_WITH", BeamBuiltinMethods.STARTS_WITH_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator CONCAT =
      createUdfOperator("CONCAT", BeamBuiltinMethods.CONCAT_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator REPLACE =
      createUdfOperator("REPLACE", BeamBuiltinMethods.REPLACE_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator TRIM =
      createUdfOperator("TRIM", BeamBuiltinMethods.TRIM_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator LTRIM =
      createUdfOperator("LTRIM", BeamBuiltinMethods.LTRIM_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator RTRIM =
      createUdfOperator("RTRIM", BeamBuiltinMethods.RTRIM_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator SUBSTR =
      createUdfOperator("SUBSTR", BeamBuiltinMethods.SUBSTR_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator REVERSE =
      createUdfOperator("REVERSE", BeamBuiltinMethods.REVERSE_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator CHAR_LENGTH =
      createUdfOperator(
          "CHAR_LENGTH", BeamBuiltinMethods.CHAR_LENGTH_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator ENDS_WITH =
      createUdfOperator(
          "ENDS_WITH", BeamBuiltinMethods.ENDS_WITH_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator LIKE =
      createUdfOperator(
          "LIKE",
          BeamBuiltinMethods.LIKE_METHOD,
          SqlSyntax.BINARY,
          ZETASQL_FUNCTION_GROUP_NAME,
          "");

  public static final SqlOperator VALIDATE_TIMESTAMP =
      createUdfOperator(
          "validateTimestamp",
          DateTimeUtils.class,
          "validateTimestamp",
          x -> NULLABLE_TIMESTAMP,
          ImmutableList.of(TIMESTAMP),
          ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator VALIDATE_TIME_INTERVAL =
      createUdfOperator(
          "validateIntervalArgument",
          DateTimeUtils.class,
          "validateTimeInterval",
          x -> NULLABLE_BIGINT,
          ImmutableList.of(BIGINT, OTHER),
          ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator TIMESTAMP_OP =
      createUdfOperator(
          "TIMESTAMP", BeamBuiltinMethods.TIMESTAMP_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator DATE_OP =
      createUdfOperator("DATE", BeamBuiltinMethods.DATE_METHOD, ZETASQL_FUNCTION_GROUP_NAME);

  public static final SqlOperator BIT_XOR =
      createUdafOperator(
          "BIT_XOR",
          x -> NULLABLE_BIGINT,
          new UdafImpl<>(new BeamBuiltinAggregations.BitXOr<Number>()));

  public static final SqlOperator COUNTIF =
      createUdafOperator(
          "countif",
          x -> createTypeFactory().createSqlType(SqlTypeName.BIGINT),
          new UdafImpl<>(new CountIf.CountIfFn()));

  public static final SqlUserDefinedFunction CAST_OP =
      new SqlUserDefinedFunction(
          new SqlIdentifier("CAST", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          null,
          new CastFunctionImpl());

  public static SqlOperator createStringAggOperator(
      ResolvedNodes.ResolvedFunctionCallBase aggregateFunctionCall) {
    List<ResolvedNodes.ResolvedExpr> args = aggregateFunctionCall.getArgumentList();
    String inputType = args.get(0).getType().typeName();
    Value delimiter = null;
    if (args.size() == 2) {
      ResolvedNodes.ResolvedExpr resolvedExpr = args.get(1);
      if (resolvedExpr instanceof ResolvedNodes.ResolvedLiteral) {
        delimiter = ((ResolvedNodes.ResolvedLiteral) resolvedExpr).getValue();
      } else {
        // TODO(https://github.com/apache/beam/issues/21283) Add support for params
        throw new ZetaSqlException(
            new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription(
                    String.format(
                        "STRING_AGG only supports ResolvedLiteral as delimiter, provided %s",
                        resolvedExpr.getClass().getName()))));
      }
    }

    switch (inputType) {
      case "BYTES":
        return SqlOperators.createUdafOperator(
            "string_agg",
            x -> SqlOperators.createTypeFactory().createSqlType(SqlTypeName.VARBINARY),
            new UdafImpl<>(
                new StringAgg.StringAggByte(
                    delimiter == null
                        ? ",".getBytes(StandardCharsets.UTF_8)
                        : delimiter.getBytesValue().toByteArray())));
      case "STRING":
        return SqlOperators.createUdafOperator(
            "string_agg",
            x -> SqlOperators.createTypeFactory().createSqlType(SqlTypeName.VARCHAR),
            new UdafImpl<>(
                new StringAgg.StringAggString(
                    delimiter == null ? "," : delimiter.getStringValue())));
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not supported in STRING_AGG", inputType));
    }
  }

  /**
   * Create a dummy SqlFunction of type OTHER_FUNCTION from given function name and return type.
   * These functions will be unparsed in either {@link
   * org.apache.beam.sdk.extensions.sql.zetasql.BeamZetaSqlCalcRel} (for built-in functions) or
   * {@link org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel} (for user-defined functions).
   */
  public static SqlFunction createZetaSqlFunction(String name, SqlTypeName returnType) {
    return new SqlFunction(
        name,
        SqlKind.OTHER_FUNCTION,
        x -> createSqlType(returnType, true),
        null, // operandTypeInference
        null, // operandTypeChecker
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  static SqlUserDefinedAggFunction createUdafOperator(
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

  private static SqlUserDefinedFunction createUdfOperator(
      String name,
      Class<?> methodClass,
      String methodName,
      SqlReturnTypeInference returnTypeInference,
      List<RelDataType> paramTypes,
      String funGroup) {
    return new SqlUserDefinedFunction(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        returnTypeInference,
        null,
        null,
        paramTypes,
        ZetaSqlScalarFunctionImpl.create(methodClass, methodName, funGroup, ""));
  }

  static SqlUserDefinedFunction createUdfOperator(
      String name, Method method, String funGroup, String jarPath) {
    return createUdfOperator(name, method, SqlSyntax.FUNCTION, funGroup, jarPath);
  }

  static SqlUserDefinedFunction createUdfOperator(String name, Method method, String funGroup) {
    return createUdfOperator(name, method, SqlSyntax.FUNCTION, funGroup, "");
  }

  private static SqlUserDefinedFunction createUdfOperator(
      String name, Method method, final SqlSyntax syntax, String funGroup, String jarPath) {
    Function function = ZetaSqlScalarFunctionImpl.create(method, funGroup, jarPath);
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
    return Lists.transform(types, type -> toSql(typeFactory, type));
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
