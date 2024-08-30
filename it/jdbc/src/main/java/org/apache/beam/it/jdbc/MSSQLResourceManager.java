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
package org.apache.beam.it.jdbc;

import java.sql.Connection;
import java.sql.Statement;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Default class for the MS SQL implementation of {@link AbstractJDBCResourceManager} abstract
 * class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is *
 * created when the container first spins up, if one is not given.
 *
 * <p>The class is thread-safe.
 */
public class MSSQLResourceManager
    extends AbstractJDBCResourceManager<MSSQLResourceManager.DefaultMSSQLServerContainer<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(MSSQLResourceManager.class);

  private static final String DEFAULT_MSSQL_CONTAINER_NAME = "mcr.microsoft.com/azure-sql-edge";

  // A list of available MSSQL Docker image tags can be found at
  // https://hub.docker.com/_/microsoft-azure-sql-edge
  private static final String DEFAULT_MSSQL_CONTAINER_TAG = "1.0.6";

  private boolean initialized;

  @SuppressWarnings("nullness")
  private MSSQLResourceManager(Builder builder) {
    super(
        new DefaultMSSQLServerContainer<>(
            DockerImageName.parse(builder.containerImageName)
                .withTag(builder.containerImageTag)
                .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server"),
            builder.databaseName,
            builder.useStaticContainer),
        builder);
    createDatabase(builder.databaseName);
  }

  @VisibleForTesting
  <T extends DefaultMSSQLServerContainer<T>> MSSQLResourceManager(T container, Builder builder) {
    super(container, builder);
    initialized = true;
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  private synchronized void createDatabase(String databaseName) {
    LOG.info("Creating database using databaseName '{}'.", databaseName);

    StringBuilder sql = new StringBuilder();
    try (Connection con = driver.getConnection(getUri(), username, password)) {
      Statement stmt = con.createStatement();
      sql.append("CREATE DATABASE ").append(databaseName);
      stmt.executeUpdate(sql.toString());
      stmt.close();
    } catch (Exception e) {
      throw new JDBCResourceManagerException(
          "Error creating database with SQL statement: " + sql, e);
    }

    initialized = true;
    LOG.info("Successfully created database {}.{}", databaseName, databaseName);
  }

  @Override
  public synchronized String getUri() {
    return String.format(
        "jdbc:%s://%s:%d%s%s",
        getJDBCPrefix(),
        this.getHost(),
        this.getPort(getJDBCPort()),
        initialized ? ";DatabaseName=" + databaseName : "",
        ";encrypt=true;trustServerCertificate=true;");
  }

  @Override
  public String getJDBCPrefix() {
    return "sqlserver";
  }

  @Override
  protected int getJDBCPort() {
    return DefaultMSSQLServerContainer.MS_SQL_SERVER_PORT;
  }

  @Override
  protected String getFirstRow(String tableName) {
    return "SELECT TOP 1 * FROM " + tableName;
  }

  /** Builder for {@link MSSQLResourceManager}. */
  public static final class Builder
      extends AbstractJDBCResourceManager.Builder<DefaultMSSQLServerContainer<?>> {

    public Builder(String testId) {
      super(testId, DEFAULT_MSSQL_CONTAINER_NAME, DEFAULT_MSSQL_CONTAINER_TAG);
    }

    @Override
    public MSSQLResourceManager build() {
      return new MSSQLResourceManager(this);
    }
  }

  static final class DefaultMSSQLServerContainer<T extends DefaultMSSQLServerContainer<T>>
      extends MSSQLServerContainer<T> {

    private final String databaseName;
    private final boolean usingStaticDatabase;

    @SuppressWarnings("nullness")
    DefaultMSSQLServerContainer(
        DockerImageName dockerImageName, String databaseName, boolean usingStaticDatabase) {
      super(dockerImageName);
      this.addEnv("ACCEPT_EULA", "Y");
      this.databaseName = databaseName;
      this.usingStaticDatabase = usingStaticDatabase;
    }

    @Override
    public T withUsername(String username) {
      if (!username.equals(AbstractJDBCResourceManager.DEFAULT_JDBC_USERNAME)
          && !usingStaticDatabase) {
        throw new UnsupportedOperationException("The username cannot be overridden to " + username);
      }
      return this.self();
    }

    @Override
    public T withDatabaseName(String dbName) {
      // AbstractJDBCResourceManager constructor calls this method, but MSSQLResourceManager
      // does
      // database creation after class initialization, so just pass container instance back as-is.
      return this.self();
    }

    @Override
    public String getDatabaseName() {
      return databaseName;
    }

    @Override
    @SuppressWarnings("nullness")
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
