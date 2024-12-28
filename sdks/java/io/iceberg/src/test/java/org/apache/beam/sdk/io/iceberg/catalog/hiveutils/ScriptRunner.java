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
package org.apache.beam.sdk.io.iceberg.catalog.hiveutils;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tool to run database scripts.
 *
 * <p>Copied over from <a
 * href="https://github.com/apache/iceberg/blob/main/hive-metastore/src/test/java/org/apache/iceberg/hive/ScriptRunner.java">Iceberg's
 * integration testing</a>
 */
@SuppressWarnings({"OperatorPrecedence", "DefaultCharset"})
public class ScriptRunner {

  private static final String DEFAULT_DELIMITER = ";";

  private final Connection connection;

  private final boolean stopOnError;
  private final boolean autoCommit;

  private final PrintWriter logWriter = new PrintWriter(System.out);
  private final PrintWriter errorLogWriter = new PrintWriter(System.err);

  /** Default constructor. */
  public ScriptRunner(Connection connection, boolean autoCommit, boolean stopOnError) {
    this.connection = connection;
    this.autoCommit = autoCommit;
    this.stopOnError = stopOnError;
  }

  /**
   * Runs an SQL script (read in using the Reader parameter).
   *
   * @param reader - the source of the script
   */
  public void runScript(Reader reader) throws IOException, SQLException {
    try {
      boolean originalAutoCommit = connection.getAutoCommit();
      try {
        if (originalAutoCommit != this.autoCommit) {
          connection.setAutoCommit(this.autoCommit);
        }
        runScript(connection, reader);
      } finally {
        connection.setAutoCommit(originalAutoCommit);
      }
    } catch (IOException | SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Error running script.  Cause: " + e, e);
    }
  }

  /**
   * Runs an SQL script (read in using the Reader parameter) using the connection passed in.
   *
   * @param conn - the connection to use for the script
   * @param reader - the source of the script
   * @throws SQLException if any SQL errors occur
   * @throws IOException if there is an error reading from the Reader
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void runScript(Connection conn, Reader reader) throws IOException, SQLException {
    StringBuilder command = null;
    try {
      LineNumberReader lineReader = new LineNumberReader(reader);
      String line;
      while ((line = lineReader.readLine()) != null) {
        if (command == null) {
          command = new StringBuilder();
        }
        String trimmedLine = line.trim();
        boolean fullLineDelimiter = false;
        if (trimmedLine.startsWith("--")) {
          println(trimmedLine);
        } else if (trimmedLine.isEmpty() || trimmedLine.startsWith("//")) {
          // Do nothing
        } else if (!fullLineDelimiter && trimmedLine.endsWith(getDelimiter())
            || fullLineDelimiter && trimmedLine.equals(getDelimiter())) {
          command.append(line, 0, line.lastIndexOf(getDelimiter()));
          command.append(" ");
          Statement statement = conn.createStatement();

          println(command);

          boolean hasResults = false;
          if (stopOnError) {
            hasResults = statement.execute(command.toString());
          } else {
            try {
              statement.execute(command.toString());
            } catch (SQLException e) {
              e.fillInStackTrace();
              printlnError("Error executing: " + command);
              printlnError(e);
            }
          }

          if (autoCommit && !conn.getAutoCommit()) {
            conn.commit();
          }

          ResultSet rs = statement.getResultSet();
          if (hasResults && rs != null) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            for (int i = 0; i < cols; i++) {
              String name = md.getColumnLabel(i);
              print(name + "\t");
            }
            println("");
            while (rs.next()) {
              for (int i = 0; i < cols; i++) {
                String value = rs.getString(i);
                print(value + "\t");
              }
              println("");
            }
          }

          command = null;
          try {
            statement.close();
          } catch (Exception e) {
            // Ignore to workaround a bug in Jakarta DBCP
          }
          Thread.yield();
        } else {
          command.append(line);
          command.append(" ");
        }
      }
      if (!autoCommit) {
        conn.commit();
      }
    } catch (IOException | SQLException e) {
      e.fillInStackTrace();
      printlnError("Error executing: " + command);
      printlnError(e);
      throw e;
    } finally {
      conn.rollback();
      flush();
    }
  }

  private String getDelimiter() {
    return DEFAULT_DELIMITER;
  }

  private void print(Object obj) {
    if (logWriter != null) {
      System.out.print(obj);
    }
  }

  private void println(Object obj) {
    if (logWriter != null) {
      logWriter.println(obj);
    }
  }

  private void printlnError(Object obj) {
    if (errorLogWriter != null) {
      errorLogWriter.println(obj);
    }
  }

  private void flush() {
    if (logWriter != null) {
      logWriter.flush();
    }
    if (errorLogWriter != null) {
      errorLogWriter.flush();
    }
  }
}
