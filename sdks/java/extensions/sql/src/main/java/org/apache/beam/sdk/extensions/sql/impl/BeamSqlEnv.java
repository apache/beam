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
package org.apache.beam.sdk.extensions.sql.impl;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.UdfUdafProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

/**
 * Contains the metadata of tables/UDF functions, and exposes APIs to
 * query/validate/optimize/translate SQL statements.
 */
@Internal
@Experimental
public class BeamSqlEnv {
  final CalciteConnection connection;
  final SchemaPlus defaultSchema;
  final BeamQueryPlanner planner;

  private BeamSqlEnv(TableProvider tableProvider) {
    connection = JdbcDriver.connect(tableProvider);
    defaultSchema = JdbcDriver.getDefaultSchema(connection);
    planner = new BeamQueryPlanner(connection);
  }

  public static BeamSqlEnv readOnly(String tableType, Map<String, BeamSqlTable> tables) {
    return withTableProvider(new ReadOnlyTableProvider(tableType, tables));
  }

  public static BeamSqlEnv withTableProvider(TableProvider tableProvider) {
    return new BeamSqlEnv(tableProvider);
  }

  public static BeamSqlEnv inMemory(TableProvider... tableProviders) {
    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    for (TableProvider tableProvider : tableProviders) {
      inMemoryMetaStore.registerProvider(tableProvider);
    }

    return withTableProvider(inMemoryMetaStore);
  }

  /** Register a UDF function which can be used in SQL expression. */
  public void registerUdf(String functionName, Class<?> clazz, String method) {
    Method[] methods = clazz.getMethods();
    for (int i = 0; i < method.length(); i++) {
      if (methods[i].getName().equals(method) && !methods[i].isBridge()) {
        defaultSchema.add(functionName, ScalarFunctionImpl.create(methods[i]));
      }
    }
  }

  /** Register a UDF function which can be used in SQL expression. */
  public void registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
    registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
  }

  /**
   * Register {@link SerializableFunction} as a UDF function which can be used in SQL expression.
   * Note, {@link SerializableFunction} must have a constructor without arguments.
   */
  public void registerUdf(String functionName, SerializableFunction sfn) {
    registerUdf(functionName, sfn.getClass(), "apply");
  }

  /**
   * Register a UDAF function which can be used in GROUP-BY expression. See {@link
   * org.apache.beam.sdk.transforms.Combine.CombineFn} on how to implement a UDAF.
   */
  public void registerUdaf(String functionName, Combine.CombineFn combineFn) {
    defaultSchema.add(functionName, new UdafImpl(combineFn));
  }

  /** Load all UDF/UDAF from {@link UdfUdafProvider}. */
  public void loadUdfUdafFromProvider() {
    ServiceLoader.<UdfUdafProvider>load(UdfUdafProvider.class)
        .forEach(
            ins -> {
              ins.getBeamSqlUdfs().forEach((udfName, udfClass) -> registerUdf(udfName, udfClass));
              ins.getSerializableFunctionUdfs()
                  .forEach((udfName, udfFn) -> registerUdf(udfName, udfFn));
              ins.getUdafs().forEach((udafName, udafFn) -> registerUdaf(udafName, udafFn));
            });
  }

  public BeamRelNode parseQuery(String query) throws ParseException {
    try {
      return planner.convertToBeamRel(query);
    } catch (ValidationException | RelConversionException | SqlParseException e) {
      throw new ParseException(String.format("Unable to parse query %s", query), e);
    }
  }

  public boolean isDdl(String sqlStatement) throws ParseException {
    try {
      return planner.parse(sqlStatement) instanceof SqlExecutableStatement;
    } catch (SqlParseException e) {
      throw new ParseException("Unable to parse statement", e);
    }
  }

  public void executeDdl(String sqlStatement) throws ParseException {
    try {
      SqlExecutableStatement ddl = (SqlExecutableStatement) planner.parse(sqlStatement);
      ddl.execute(getContext());
    } catch (SqlParseException e) {
      throw new ParseException("Unable to parse DDL statement", e);
    }
  }

  public CalcitePrepare.Context getContext() {
    return connection.createPrepareContext();
  }

  public Map<String, String> getPipelineOptions() {
    return ((BeamCalciteSchema) CalciteSchema.from(defaultSchema).schema).getPipelineOptions();
  }

  public String explain(String sqlString) throws ParseException {
    try {
      return RelOptUtil.toString(planner.convertToBeamRel(sqlString));
    } catch (ValidationException | RelConversionException | SqlParseException e) {
      throw new ParseException("Unable to parse statement", e);
    }
  }
}
