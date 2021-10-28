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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaConnection;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.AvaticaStatement;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.Meta;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.QueryState;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteSchema;

/**
 * Wrapper for {@link CalciteFactory}.
 *
 * <p>This is a non-functional class to delegate to the underlying {@link CalciteFactory}. The
 * purpose is to hide the delegation logic from the implementation ({@link JdbcFactory}).
 */
public abstract class CalciteFactoryWrapper extends CalciteFactory {

  protected CalciteFactory factory;

  CalciteFactoryWrapper(CalciteFactory factory) {
    super(4, 1);
    this.factory = factory;
  }

  @Override
  public AvaticaConnection newConnection(
      UnregisteredDriver driver,
      AvaticaFactory avaticaFactory,
      String url,
      Properties info,
      CalciteSchema rootSchema,
      JavaTypeFactory typeFactory) {

    return this.factory.newConnection(driver, avaticaFactory, url, info, rootSchema, typeFactory);
  }

  @Override
  public AvaticaStatement newStatement(
      AvaticaConnection connection,
      Meta.StatementHandle h,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    return this.factory.newStatement(
        connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public AvaticaPreparedStatement newPreparedStatement(
      AvaticaConnection connection,
      Meta.StatementHandle h,
      Meta.Signature signature,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    return this.factory.newPreparedStatement(
        connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public AvaticaResultSet newResultSet(
      AvaticaStatement statement,
      QueryState state,
      Meta.Signature signature,
      TimeZone timeZone,
      Meta.Frame firstFrame)
      throws SQLException {
    return this.factory.newResultSet(statement, state, signature, timeZone, firstFrame);
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
    return this.factory.newDatabaseMetaData(connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(
      AvaticaStatement statement, Meta.Signature signature) throws SQLException {
    return this.factory.newResultSetMetaData(statement, signature);
  }
}
