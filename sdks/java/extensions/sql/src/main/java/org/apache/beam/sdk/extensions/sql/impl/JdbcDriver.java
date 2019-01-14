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

import static org.apache.calcite.avatica.BuiltInConnectionProperty.TIME_ZONE;
import static org.apache.calcite.config.CalciteConnectionProperty.LEX;
import static org.apache.calcite.config.CalciteConnectionProperty.PARSER_FACTORY;
import static org.apache.calcite.config.CalciteConnectionProperty.SCHEMA;
import static org.apache.calcite.config.CalciteConnectionProperty.SCHEMA_FACTORY;
import static org.apache.calcite.config.CalciteConnectionProperty.TYPE_SYSTEM;
import static org.codehaus.commons.compiler.CompilerFactoryFactory.getDefaultCompilerFactory;

import com.google.auto.service.AutoService;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.tools.RuleSet;

/**
 * Calcite JDBC driver with Beam defaults.
 *
 * <p>Connection URLs have this form:
 *
 * <p><code>jdbc:beam:param1=value1;param2=value2;param3=value3</code>
 *
 * <p>The querystring-style parameters are parsed as {@link PipelineOptions}.
 */
@AutoService(java.sql.Driver.class)
public class JdbcDriver extends Driver {
  public static final JdbcDriver INSTANCE = new JdbcDriver();
  public static final String CONNECT_STRING_PREFIX = "jdbc:beam:";
  static final String TOP_LEVEL_BEAM_SCHEMA = "beam";

  static {
    ClassLoader origLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(JdbcDriver.class.getClassLoader());

      // init the compiler factory using correct class loader
      getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(origLoader);
    }
    // inject beam rules into planner
    Hook.PLANNER.add(
        (Consumer<RelOptPlanner>)
            planner -> {
              for (RuleSet ruleSet : BeamRuleSets.getRuleSets()) {
                for (RelOptRule rule : ruleSet) {
                  planner.addRule(rule);
                }
              }
              planner.removeRule(CalcRemoveRule.INSTANCE);
              planner.removeRule(SortRemoveRule.INSTANCE);

              for (RelOptRule rule : CalcitePrepareImpl.ENUMERABLE_RULES) {
                planner.removeRule(rule);
              }

              List<RelTraitDef> relTraitDefs = new ArrayList<>(planner.getRelTraitDefs());
              planner.clearRelTraitDefs();
              for (RelTraitDef def : relTraitDefs) {
                if (!(def instanceof RelCollationTraitDef)) {
                  planner.addRelTraitDef(def);
                }
              }
            });
    // register JDBC driver
    INSTANCE.register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  /**
   * Configures Beam-specific options and opens a JDBC connection to Calcite.
   *
   * <p>If {@code originalConnectionProperties} doesn't have the Beam-specific properties, populates
   * them with defaults (e.g. sets the default schema name to "beam").
   *
   * <p>Returns null if {@code url} doesn't begin with {@link #CONNECT_STRING_PREFIX}. This seems to
   * be how JDBC decides whether a driver can handle a request. It tries to connect to it, and if
   * the result is null it picks another driver.
   *
   * <p>Returns an instance of {@link JdbcConnection} which is a Beam wrapper around {@link
   * CalciteConnection}.
   */
  @Override
  public Connection connect(String url, Properties originalConnectionProperties)
      throws SQLException {

    // do this check before even looking into properties
    // do not remove this, please
    if (!acceptsURL(url)) {
      return null;
    }

    Properties connectionProps = ensureDefaultProperties(originalConnectionProperties);
    CalciteConnection calciteConnection = (CalciteConnection) super.connect(url, connectionProps);

    // calciteConnection is initialized with an empty Beam schema,
    // we need to populate it with pipeline options, load table providers, etc
    return JdbcConnection.initialize(calciteConnection);
  }

  /**
   * Make sure required default properties are set.
   *
   * <p>Among other things sets up the parser class name, rel data type system and default schema
   * factory.
   *
   * <p>The specified Beam schema factory will be used by Calcite to create the initial top level
   * Beam schema. It can be later overridden by setting the schema via {@link
   * JdbcConnection#setSchema(String, TableProvider)}.
   */
  private Properties ensureDefaultProperties(Properties originalInfo) {
    Properties info = new Properties();
    info.putAll(originalInfo);

    setIfNull(info, TIME_ZONE, "UTC");
    setIfNull(info, LEX, Lex.JAVA.name());
    setIfNull(info, PARSER_FACTORY, BeamSqlParserImpl.class.getName() + "#FACTORY");
    setIfNull(info, TYPE_SYSTEM, BeamRelDataTypeSystem.class.getName());
    setIfNull(info, SCHEMA, TOP_LEVEL_BEAM_SCHEMA);
    setIfNull(info, SCHEMA_FACTORY, BeamCalciteSchemaFactory.AllProviders.class.getName());

    info.put("beam.userAgent", "BeamSQL/" + ReleaseInfo.getReleaseInfo().getVersion());

    return info;
  }

  private static void setIfNull(Properties info, ConnectionProperty key, String value) {
    // A null value indicates the default. We want to override defaults only.
    if (info.getProperty(key.camelName()) == null) {
      info.setProperty(key.camelName(), value);
    }
  }

  /**
   * Connects to the driver using standard {@link #connect(String, Properties)} call, but overrides
   * the initial schema factory. Default factory would load up all table providers. The one
   * specified here doesn't load any providers. We then override the top-level schema with the
   * {@code tableProvider}.
   *
   * <p>This is called in tests and {@link BeamSqlEnv}, core part of {@link SqlTransform}. CLI uses
   * standard JDBC driver registry, and goes through {@link #connect(String, Properties)} instead,
   * not this path. The CLI ends up using the schema factory that populates the default schema with
   * all table providers it can find. See {@link BeamCalciteSchemaFactory}.
   */
  public static JdbcConnection connect(TableProvider tableProvider) {
    try {
      Properties properties = new Properties();
      setIfNull(properties, SCHEMA_FACTORY, BeamCalciteSchemaFactory.Empty.class.getName());
      JdbcConnection connection =
          (JdbcConnection) INSTANCE.connect(CONNECT_STRING_PREFIX, properties);
      connection.setSchema(TOP_LEVEL_BEAM_SCHEMA, tableProvider);
      return connection;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
