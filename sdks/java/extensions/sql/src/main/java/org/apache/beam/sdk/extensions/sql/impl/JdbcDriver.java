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

import static org.codehaus.commons.compiler.CompilerFactoryFactory.getDefaultCompilerFactory;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
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

  /**
   * Querystring parameters that begin with {@code "beam."} will be interpreted as {@link
   * PipelineOptions}.
   */
  public static final String BEAM_QUERYSTRING_PREFIX = "beam.";

  private static final String BEAM_CALCITE_SCHEMA = "beamCalciteSchema";

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

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    final BeamCalciteSchema beamCalciteSchema = (BeamCalciteSchema) info.get(BEAM_CALCITE_SCHEMA);

    Properties info2 = new Properties(info);
    setDefault(info2, BuiltInConnectionProperty.TIME_ZONE, "UTC");
    setDefault(info2, CalciteConnectionProperty.LEX, Lex.JAVA.name());
    setDefault(
        info2,
        CalciteConnectionProperty.PARSER_FACTORY,
        BeamSqlParserImpl.class.getName() + "#FACTORY");
    setDefault(info2, CalciteConnectionProperty.TYPE_SYSTEM, BeamRelDataTypeSystem.class.getName());
    setDefault(info2, CalciteConnectionProperty.SCHEMA, "beam");
    setDefault(
        info2, CalciteConnectionProperty.SCHEMA_FACTORY, BeamCalciteSchemaFactory.class.getName());

    CalciteConnection connection = (CalciteConnection) super.connect(url, info2);
    final SchemaPlus defaultSchema;
    if (beamCalciteSchema != null) {
      defaultSchema =
          connection.getRootSchema().add(connection.config().schema(), beamCalciteSchema);
      connection.setSchema(defaultSchema.getName());
    } else {
      defaultSchema = getDefaultSchema(connection);
    }

    // Beam schema may change without notifying Calcite
    defaultSchema.setCacheEnabled(false);

    // Set default PipelineOptions to which we apply the querystring
    Map<String, String> pipelineOptionsMap =
        ((BeamCalciteSchema) CalciteSchema.from(defaultSchema).schema).getPipelineOptions();
    ReleaseInfo releaseInfo = ReleaseInfo.getReleaseInfo();
    pipelineOptionsMap.put("userAgent", String.format("BeamSQL/%s", releaseInfo.getVersion()));

    String querystring = url.substring(CONNECT_STRING_PREFIX.length());
    for (Map.Entry<Object, Object> propertyValue :
        ConnectStringParser.parse(querystring).entrySet()) {
      String name = (String) propertyValue.getKey();
      if (name.startsWith(BEAM_QUERYSTRING_PREFIX)) {
        pipelineOptionsMap.put(
            name.substring(BEAM_QUERYSTRING_PREFIX.length()), (String) propertyValue.getValue());
      }
    }
    return connection;
  }

  private static void setDefault(Properties info, ConnectionProperty key, String value) {
    // A null value indicates the default. We want to override defaults only.
    if (info.getProperty(key.camelName()) == null) {
      info.setProperty(key.camelName(), value);
    }
  }

  @VisibleForTesting
  public static CalciteConnection connect(TableProvider tableProvider) {
    try {
      Properties info = new Properties();
      info.put(BEAM_CALCITE_SCHEMA, new BeamCalciteSchema(tableProvider));
      return (CalciteConnection) INSTANCE.connect(CONNECT_STRING_PREFIX, info);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static SchemaPlus getDefaultSchema(CalciteConnection connection) {
    try {
      String defaultSchemaName = connection.getSchema();
      return connection.getRootSchema().getSubSchema(defaultSchemaName);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
