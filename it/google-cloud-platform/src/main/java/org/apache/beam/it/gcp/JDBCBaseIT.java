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
package org.apache.beam.it.gcp;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JDBCBaseIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCBaseIT.class);

  // The sub-folder to store the jars in GCS.
  private static final String GCS_PREFIX = "jars/";

  // The .jar suffix for generating JDBC jar names
  private static final String JAR_SUFFIX = ".jar";

  // The JDBC Driver fully-qualified class names
  protected static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
  protected static final String POSTGRES_DRIVER = "org.postgresql.Driver";
  protected static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
  protected static final String MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  // The relative path to the JDBC drivers under Maven's `.m2/repository` directory
  private static final String MYSQL_LOCAL_PATH = "mysql/mysql-connector-java";
  private static final String POSTGRES_LOCAL_PATH = "org/postgresql/postgresql";
  private static final String ORACLE_LOCAL_PATH = "com/oracle/database/jdbc/ojdbc8";
  private static final String MSSQL_LOCAL_PATH = "com/microsoft/sqlserver/mssql-jdbc";

  // The name of the JDBC driver jars sans the .jar suffix
  private static final String MYSQL_JAR_PREFIX = "mysql-connector-java";
  private static final String POSTGRES_JAR_PREFIX = "postgresql";
  private static final String ORACLE_JAR_PREFIX = "ojdbc8";
  private static final String MSSQL_JAR_PREFIX = "mssql-jdbc";

  // The versions of each of the JDBC drivers to use.
  // NOTE: These versions must correspond to the versions declared in the `it/pom.xml` file.
  private static final String MYSQL_VERSION = "8.0.30";
  private static final String POSTGRES_VERSION = "42.6.0";
  private static final String ORACLE_VERSION = "23.2.0.0";
  private static final String MSSQL_VERSION = "12.2.0.jre11";

  @Before
  public void setUpJDBC() throws IOException {

    String basePath = getMvnBaseRepoPath();

    String mySqlDriverGCSRelativePath = GCS_PREFIX + mySqlDriverLocalJar();
    String postgresDriverGCSRelativePath = GCS_PREFIX + postgresDriverLocalJar();
    String oracleDriverGCSRelativePath = GCS_PREFIX + oracleDriverLocalJar();
    String msSqlDriverGCSRelativePath = GCS_PREFIX + msSqlDriverLocalJar();

    gcsClient.uploadArtifact(mySqlDriverGCSRelativePath, mySqlDriverLocalPath(basePath));
    gcsClient.uploadArtifact(postgresDriverGCSRelativePath, postgresDriverLocalPath(basePath));
    gcsClient.uploadArtifact(oracleDriverGCSRelativePath, oracleDriverLocalPath(basePath));
    gcsClient.uploadArtifact(msSqlDriverGCSRelativePath, msSqlDriverLocalPath(basePath));
  }

  protected String mySqlDriverGCSPath() {
    return getGcsPath(GCS_PREFIX + mySqlDriverLocalJar());
  }

  protected String postgresDriverGCSPath() {
    return getGcsPath(GCS_PREFIX + postgresDriverLocalJar());
  }

  protected String oracleDriverGCSPath() {
    return getGcsPath(GCS_PREFIX + oracleDriverLocalJar());
  }

  protected String msSqlDriverGCSPath() {
    return getGcsPath(GCS_PREFIX + msSqlDriverLocalJar());
  }

  private static String mySqlDriverLocalJar() {
    return String.join("-", MYSQL_JAR_PREFIX, MYSQL_VERSION) + JAR_SUFFIX;
  }

  private static String postgresDriverLocalJar() {
    return String.join("-", POSTGRES_JAR_PREFIX, POSTGRES_VERSION) + JAR_SUFFIX;
  }

  private static String oracleDriverLocalJar() {
    return String.join("-", ORACLE_JAR_PREFIX, ORACLE_VERSION) + JAR_SUFFIX;
  }

  private static String msSqlDriverLocalJar() {
    return String.join("-", MSSQL_JAR_PREFIX, MSSQL_VERSION) + JAR_SUFFIX;
  }

  private static String mySqlDriverLocalPath(String basePath) {
    return String.join("/", basePath, MYSQL_LOCAL_PATH, MYSQL_VERSION, mySqlDriverLocalJar());
  }

  private static String postgresDriverLocalPath(String basePath) {
    return String.join(
        "/", basePath, POSTGRES_LOCAL_PATH, POSTGRES_VERSION, postgresDriverLocalJar());
  }

  private static String oracleDriverLocalPath(String basePath) {
    return String.join("/", basePath, ORACLE_LOCAL_PATH, ORACLE_VERSION, oracleDriverLocalJar());
  }

  private static String msSqlDriverLocalPath(String basePath) {
    return String.join("/", basePath, MSSQL_LOCAL_PATH, MSSQL_VERSION, msSqlDriverLocalJar());
  }

  private String getMvnBaseRepoPath() {
    // Try to get specified maven repo path from args
    if (System.getProperty("mavenRepository") != null) {
      String basePath = System.getProperty("mavenRepository");
      return basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;
    }

    // Default to Maven settings.localRepository
    String mavenCmd = "mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout";
    try {
      Process exec = Runtime.getRuntime().exec(mavenCmd);
      IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);
      InputStream inStream = exec.getInputStream();

      if (exec.waitFor() != 0) {
        throw new RuntimeException("Error retrieving Maven repo path, check Maven logs.");
      }

      byte[] allBytes = inStream.readAllBytes();
      String basePath = new String(allBytes, StandardCharsets.UTF_8);

      // Remove color reset chars "\033[0m" sent to terminal output
      basePath =
          basePath.replace(new String(new byte[] {27, 91, 48, 109}, StandardCharsets.UTF_8), "");

      inStream.close();

      return basePath;

    } catch (Exception e) {
      throw new IllegalArgumentException("Error retrieving Maven repo path", e);
    }
  }
}
