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

import static org.apache.beam.sdk.extensions.sql.impl.JdbcDriver.TOP_LEVEL_BEAM_SCHEMA;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.BuiltInConnectionProperty.TIME_ZONE;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.config.CalciteConnectionProperty.LEX;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.config.CalciteConnectionProperty.PARSER_FACTORY;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.config.CalciteConnectionProperty.SCHEMA;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.config.CalciteConnectionProperty.SCHEMA_FACTORY;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.config.CalciteConnectionProperty.TYPE_SYSTEM;

import java.util.Properties;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaConnection;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.ConnectionProperty;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteSchema;

/**
 * Implements {@link CalciteFactory} that is used by Clacite JDBC driver to instantiate different
 * JDBC objects, like connections, result sets, etc.
 *
 * <p>The purpose of this class is to intercept the connection creation and force a cache-less root
 * schema ({@link
 * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.SimpleCalciteSchema}). Otherwise
 * Calcite uses {@link
 * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CachingCalciteSchema} that eagerly
 * caches table information. This behavior does not work well for dynamic table providers.
 */
class JdbcFactory extends CalciteFactoryWrapper {

  JdbcFactory(CalciteFactory factory) {
    super(factory);
  }

  static JdbcFactory wrap(CalciteFactory calciteFactory) {
    return new JdbcFactory(calciteFactory);
  }

  @Override
  public AvaticaConnection newConnection(
      UnregisteredDriver driver,
      AvaticaFactory avaticaFactory,
      String url,
      Properties info,
      CalciteSchema rootSchema,
      JavaTypeFactory typeFactory) {

    Properties connectionProps = ensureDefaultProperties(info);
    CalciteSchema actualRootSchema = rootSchema;
    if (rootSchema == null) {
      actualRootSchema = CalciteSchema.createRootSchema(true, false, "");
    }

    return super.newConnection(
        driver, avaticaFactory, url, connectionProps, actualRootSchema, typeFactory);
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
    setIfNull(info, "beam.userAgent", "BeamSQL/" + ReleaseInfo.getReleaseInfo().getVersion());

    return info;
  }

  private static void setIfNull(Properties info, ConnectionProperty key, String value) {
    setIfNull(info, key.camelName(), value);
  }

  private static void setIfNull(Properties info, String key, String value) {
    // A null value indicates the default. We want to override defaults only.
    if (info.getProperty(key) == null) {
      info.setProperty(key, value);
    }
  }
}
