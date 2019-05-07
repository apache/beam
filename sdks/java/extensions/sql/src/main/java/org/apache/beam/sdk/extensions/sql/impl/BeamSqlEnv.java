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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.udf.BeamBuiltinFunctionProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.UdfUdafProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlExecutableStatement;

/**
 * Contains the metadata of tables/UDF functions, and exposes APIs to
 * query/validate/optimize/translate SQL statements.
 */
@Internal
@Experimental
public class BeamSqlEnv {
  final JdbcConnection connection;
  final QueryPlanner planner;

  private BeamSqlEnv(JdbcConnection connection, QueryPlanner planner) {
    this.connection = connection;
    this.planner = planner;
  }

  public static BeamSqlEnvBuilder builder() {
    return new BeamSqlEnvBuilder();
  }

  public static BeamSqlEnv readOnly(String tableType, Map<String, BeamSqlTable> tables) {
    return withTableProvider(new ReadOnlyTableProvider(tableType, tables));
  }

  public static BeamSqlEnv withTableProvider(TableProvider tableProvider) {
    return builder().setInitializeTableProvider(tableProvider).build();
  }

  public static BeamSqlEnv inMemory(TableProvider... tableProviders) {
    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    for (TableProvider tableProvider : tableProviders) {
      inMemoryMetaStore.registerProvider(tableProvider);
    }

    return withTableProvider(inMemoryMetaStore);
  }

  public BeamRelNode parseQuery(String query) throws ParseException {
    return planner.convertToBeamRel(query);
  }

  public boolean isDdl(String sqlStatement) throws ParseException {
    return planner.parse(sqlStatement) instanceof SqlExecutableStatement;
  }

  public void executeDdl(String sqlStatement) throws ParseException {
    SqlExecutableStatement ddl = (SqlExecutableStatement) planner.parse(sqlStatement);
    ddl.execute(getContext());
  }

  public CalcitePrepare.Context getContext() {
    return connection.createPrepareContext();
  }

  public Map<String, String> getPipelineOptions() {
    return connection.getPipelineOptionsMap();
  }

  public String explain(String sqlString) throws ParseException {
    try {
      return RelOptUtil.toString(planner.convertToBeamRel(sqlString));
    } catch (Exception e) {
      throw new ParseException("Unable to parse statement", e);
    }
  }

  /** BeamSqlEnv's Builder. */
  public static class BeamSqlEnvBuilder {
    private String queryPlannerClassName =
        "org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner";

    private TableProvider initialTableProvider;
    private String currentSchemaName;
    private Map<String, TableProvider> schemaMap = new HashMap<>();
    private Set<Map.Entry<String, Function>> functionSet = new HashSet<>();

    public BeamSqlEnvBuilder setInitializeTableProvider(TableProvider tableProvider) {
      initialTableProvider = tableProvider;
      return this;
    }

    public BeamSqlEnvBuilder registerBuiltinUdf(Map<String, List<Method>> methods) {
      for (Map.Entry<String, List<Method>> entry : methods.entrySet()) {
        for (Method method : entry.getValue()) {
          functionSet.add(new SimpleEntry<>(entry.getKey(), UdfImpl.create(method)));
        }
      }
      return this;
    }

    public BeamSqlEnvBuilder addSchema(String name, TableProvider tableProvider) {
      if (schemaMap.containsKey(name)) {
        throw new RuntimeException("Schema " + name + " is registered twice.");
      }

      schemaMap.put(name, tableProvider);
      return this;
    }

    public BeamSqlEnvBuilder setCurrentSchema(String name) {
      currentSchemaName = name;
      return this;
    }

    /** Register a UDF function which can be used in SQL expression. */
    public BeamSqlEnvBuilder registerUdf(String functionName, Class<?> clazz, String method) {
      functionSet.add(new SimpleEntry<>(functionName, UdfImpl.create(clazz, method)));

      return this;
    }

    /** Register a UDF function which can be used in SQL expression. */
    public BeamSqlEnvBuilder registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
      return registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
    }

    public BeamSqlEnvBuilder registerUdf(String functionName, SerializableFunction sfn) {
      return registerUdf(functionName, sfn.getClass(), "apply");
    }

    /**
     * Register a UDAF function which can be used in GROUP-BY expression. See {@link
     * org.apache.beam.sdk.transforms.Combine.CombineFn} on how to implement a UDAF.
     */
    public BeamSqlEnvBuilder registerUdaf(String functionName, Combine.CombineFn combineFn) {
      functionSet.add(new SimpleEntry<>(functionName, new UdafImpl(combineFn)));
      return this;
    }

    /** Load all UDF/UDAF from {@link UdfUdafProvider}. */
    public BeamSqlEnvBuilder loadUdfUdafFromProvider() {
      ServiceLoader.<UdfUdafProvider>load(UdfUdafProvider.class)
          .forEach(
              ins -> {
                ins.getBeamSqlUdfs().forEach((udfName, udfClass) -> registerUdf(udfName, udfClass));
                ins.getSerializableFunctionUdfs()
                    .forEach((udfName, udfFn) -> registerUdf(udfName, udfFn));
                ins.getUdafs().forEach((udafName, udafFn) -> registerUdaf(udafName, udafFn));
              });

      return this;
    }

    public BeamSqlEnvBuilder loadBeamBuiltinFunctions() {
      for (BeamBuiltinFunctionProvider provider :
          ServiceLoader.load(BeamBuiltinFunctionProvider.class)) {
        registerBuiltinUdf(provider.getBuiltinMethods());
      }

      return this;
    }

    public BeamSqlEnvBuilder setQueryPlannerClassName(String name) {
      queryPlannerClassName = name;
      return this;
    }

    /**
     * Build function to create an instance of BeamSqlEnv based on preset fields.
     *
     * @return BeamSqlEnv.
     */
    public BeamSqlEnv build() {
      // This check is to retain backward compatible because most of BeamSqlEnv are initialized by
      // withTableProvider API.
      if (initialTableProvider == null) {
        throw new RuntimeException("initialTableProvider must be set in BeamSqlEnvBuilder.");
      }

      JdbcConnection jdbcConnection = JdbcDriver.connect(initialTableProvider);

      // set schema
      for (Map.Entry<String, TableProvider> schemaEntry : schemaMap.entrySet()) {
        jdbcConnection.setSchema(schemaEntry.getKey(), schemaEntry.getValue());
      }

      // reset default schema
      if (currentSchemaName != null) {
        try {
          jdbcConnection.setSchema(currentSchemaName);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      // add UDF
      for (Map.Entry<String, Function> functionEntry : functionSet) {
        jdbcConnection.getCurrentSchemaPlus().add(functionEntry.getKey(), functionEntry.getValue());
      }

      QueryPlanner planner;

      if (queryPlannerClassName.equals(
          "org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner")) {
        planner = new CalciteQueryPlanner(jdbcConnection);
      } else {
        try {
          planner =
              (QueryPlanner)
                  Class.forName(queryPlannerClassName)
                      .getConstructor(JdbcConnection.class)
                      .newInstance(jdbcConnection);
        } catch (NoSuchMethodException
            | ClassNotFoundException
            | InstantiationException
            | IllegalAccessException
            | InvocationTargetException e) {
          throw new RuntimeException(
              String.format("Cannot construct query planner %s", queryPlannerClassName), e);
        }
      }

      return new BeamSqlEnv(jdbcConnection, planner);
    }
  }
}
