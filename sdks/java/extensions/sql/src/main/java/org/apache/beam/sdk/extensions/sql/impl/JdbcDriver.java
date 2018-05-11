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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Calcite JDBC driver with Beam defaults.
 */
public class JdbcDriver extends Driver {
  public static final JdbcDriver INSTANCE = new JdbcDriver();
  public static final String CONNECT_STRING_PREFIX = "jdbc:beam:";

  static {
    INSTANCE.register();
  }

  @Override protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    Properties info2 = new Properties(info);
    setDefault(info2, CalciteConnectionProperty.LEX, Lex.JAVA.name());
    setDefault(info2, CalciteConnectionProperty.PARSER_FACTORY,
        BeamSqlParserImpl.class.getName() + "#FACTORY");
    setDefault(info2, CalciteConnectionProperty.TYPE_SYSTEM,
        BeamRelDataTypeSystem.class.getName());
    setDefault(info2, CalciteConnectionProperty.SCHEMA, "beam");
    setDefault(info2, CalciteConnectionProperty.SCHEMA_FACTORY,
        BeamCalciteSchemaFactory.class.getName());

    CalciteConnection connection = (CalciteConnection) super.connect(url, info2);
    final SchemaPlus defaultSchema = connection.getRootSchema()
        .getSubSchema(connection.getSchema());

    // Beam schema may change without notifying Calcite
    defaultSchema.setCacheEnabled(false);
    return connection;
  }

  private static void setDefault(Properties info, ConnectionProperty key, String value) {
    // A null value indicates the default. We want to override defaults only.
    if (info.getProperty(key.camelName()) == null) {
      info.setProperty(key.camelName(), value);
    }
  }
}
