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
package org.apache.beam.sdk.io.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.LogRecord;
import javax.sql.DataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class JdbcIOExceptionHandlingParameterizedTest {
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(JdbcIO.class);

  private static final JdbcIO.DataSourceConfiguration DATA_SOURCE_CONFIGURATION =
      JdbcIO.DataSourceConfiguration.create(
          "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:testDB;create=true");
  private static final DataSource DATA_SOURCE = DATA_SOURCE_CONFIGURATION.buildDatasource();

  @BeforeClass
  public static void beforeClass() {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {new SQLException("SQL deadlock", "40001"), null, 4},
          {new SQLException("PostgreSQL deadlock", "40P01"), null, 4},
          {new SQLException("NOT A deadlock", "40234"), null, 1},
          {new SQLException("PostgreSQL NOT A deadlock", "40P12"), null, 1},
          {null, new SQLException("SQL deadlock", "40001"), 4},
          {null, new SQLException("PostgreSQL deadlock", "40P01"), 4},
          {null, new SQLException("NOT A deadlock", "40234"), 1},
          {null, new SQLException("PostgreSQL NOT A deadlock", "40P12"), 1},
        });
  }

  private final Exception executeBatchException;
  private final Exception commitException;
  private final Integer retries;

  public JdbcIOExceptionHandlingParameterizedTest(
      Exception executeBatchException, Exception commitException, Integer retries) {
    this.executeBatchException = executeBatchException;
    this.commitException = commitException;
    this.retries = retries;
  }

  @Test
  public void testExceptionsAndRetries() throws Exception {
    String tableName = DatabaseTestHelper.getTestTableName("UT_EXCEPTION_HANDLING");
    DatabaseTestHelper.createTable(DATA_SOURCE, tableName);

    DataSource mockedDs = Mockito.mock(DataSource.class, Mockito.withSettings().serializable());
    Connection mockedConn = Mockito.mock(Connection.class, Mockito.withSettings().serializable());
    PreparedStatement mockedStatement =
        Mockito.mock(PreparedStatement.class, Mockito.withSettings().serializable());
    Mockito.when(mockedDs.getConnection()).thenReturn(mockedConn);
    Mockito.when(mockedConn.prepareStatement(Mockito.anyString())).thenReturn(mockedStatement);
    SerializableFunction<Void, DataSource> dsprovider = (vd) -> mockedDs;

    final String excMessage;
    if (executeBatchException != null) {
      Mockito.when(mockedStatement.executeBatch()).thenThrow(executeBatchException);
      excMessage = executeBatchException.getMessage();
    } else if (commitException != null) {
      Mockito.doThrow(commitException).when(mockedConn).commit();
      excMessage = commitException.getMessage();
    } else {
      excMessage = "";
    }

    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(Collections.singletonList(KV.of(1, "TEST"))))
        .apply(
            JdbcIO.<KV<Integer, String>>write()
                .withDataSourceProviderFn(dsprovider)
                .withRetryConfiguration(
                    JdbcIO.RetryConfiguration.create(3, Duration.millis(1000), Duration.millis(1)))
                .withStatement(String.format("insert into %s values(?, ?)", tableName))
                .withPreparedStatementSetter(
                    (element, statement) -> {
                      statement.setInt(1, element.getKey());
                      statement.setString(2, element.getValue());
                    }));
    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              pipeline.run().waitUntilFinish();
            });
    assertThat(exception.getMessage(), containsString(excMessage));
    exception.printStackTrace();

    expectedLogs.verifyLogRecords(
        new TypeSafeMatcher<Iterable<LogRecord>>() {
          @Override
          public void describeTo(Description description) {}

          @Override
          protected boolean matchesSafely(Iterable<LogRecord> logRecords) {
            int count = 0;
            for (LogRecord logRecord : logRecords) {
              if (logRecord.getMessage().contains(excMessage)) {
                count += 1;
              }
            }
            return count == retries;
          }
        });
  }
}
