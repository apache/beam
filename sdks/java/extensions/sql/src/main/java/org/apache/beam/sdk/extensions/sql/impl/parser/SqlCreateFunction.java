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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Static.RESOURCE;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.extensions.sql.impl.JavaUdfLoader;
import org.apache.beam.sdk.extensions.sql.impl.LazyAggregateCombineFn;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFnReflector;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.udf.ScalarFn;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlCreate;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Parse tree for {@code CREATE FUNCTION} statement. */
public class SqlCreateFunction extends SqlCreate implements BeamSqlParser.ExecutableStatement {
  private final boolean isAggregate;
  private final SqlIdentifier functionName;
  private final SqlNode jarPath;

  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

  /** Creates a SqlCreateFunction. */
  public SqlCreateFunction(
      SqlParserPos pos,
      boolean replace,
      SqlIdentifier functionName,
      SqlNode jarPath,
      boolean isAggregate) {
    super(OPERATOR, pos, replace, false);
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.jarPath = Objects.requireNonNull(jarPath, "jarPath");
    this.isAggregate = isAggregate;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (isAggregate) {
      writer.keyword("AGGREGATE");
    }
    writer.keyword("FUNCTION");
    functionName.unparse(writer, 0, 0);
    writer.keyword("USING JAR");
    jarPath.unparse(writer, 0, 0);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(functionName, jarPath);
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, functionName);
    SchemaPlus schema = pair.left.plus();
    String lastName = pair.right;
    if (!schema.getFunctions(lastName).isEmpty()) {
      throw SqlUtil.newContextException(
          functionName.getParserPosition(),
          RESOURCE.internal(String.format("Function %s is already defined.", lastName)));
    }
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    // TODO(https://github.com/apache/beam/issues/20834) Support qualified function names.
    List<String> functionPath = ImmutableList.of(lastName);
    if (!(jarPath instanceof SqlCharStringLiteral)) {
      throw SqlUtil.newContextException(
          jarPath.getParserPosition(),
          RESOURCE.internal("Jar path is not instanceof SqlCharStringLiteral."));
    }
    String unquotedJarPath = ((SqlCharStringLiteral) jarPath).getNlsString().getValue();
    if (isAggregate) {
      // Try loading the aggregate function just to make sure it exists. LazyAggregateCombineFn will
      // need to fetch it again at runtime.
      udfLoader.loadAggregateFunction(functionPath, unquotedJarPath);
      LazyAggregateCombineFn<?, ?, ?> combineFn =
          new LazyAggregateCombineFn<>(functionPath, unquotedJarPath);
      schema.add(lastName, combineFn.getUdafImpl());
    } else {
      ScalarFn scalarFn = udfLoader.loadScalarFunction(functionPath, unquotedJarPath);
      Method method = ScalarFnReflector.getApplyMethod(scalarFn);
      Function function = ScalarFunctionImpl.create(method, unquotedJarPath);
      schema.add(lastName, function);
    }
  }
}
