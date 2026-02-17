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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ChangeStreamDaoTest {
  private DatabaseClient databaseClient;
  private RpcPriority rpcPriority;
  private static final String CHANGE_STREAM_NAME = "testCS";

  @Before
  public void setUp() {
    databaseClient = mock(DatabaseClient.class);
    rpcPriority = mock(RpcPriority.class);
  }

  // New tests for PostgreSQL branch to verify the chosen TVF in the generated
  // SQL.
  @Test
  public void testChangeStreamQueryPostgresMutable() {
    // Arrange: single-use transaction for the actual change stream query
    ReadOnlyTransaction singleUseTx = mock(ReadOnlyTransaction.class);
    when(databaseClient.singleUse()).thenReturn(singleUseTx);

    ChangeStreamDao changeStreamDao =
        new ChangeStreamDao(
            CHANGE_STREAM_NAME, databaseClient, rpcPriority, "testjob", Dialect.POSTGRESQL, true);

    // Act: call the method that constructs and executes the statement
    changeStreamDao.changeStreamQuery(null, null, null, 0L);

    // Assert: capture the Statement passed to singleUse().executeQuery and verify
    // SQL
    ArgumentCaptor<Statement> captor = ArgumentCaptor.forClass(Statement.class);
    verify(singleUseTx).executeQuery(captor.capture(), any(), any());
    Statement captured = captor.getValue();
    String sql = captured.getSql(); // adjust if different accessor is used
    assertTrue(
        "Expected SQL to contain read_proto_bytes_",
        sql.contains("read_proto_bytes_" + CHANGE_STREAM_NAME));
  }

  @Test
  public void testChangeStreamQueryPostgresImmutable() {
    // Arrange: single-use transaction for the actual change stream query
    ReadOnlyTransaction singleUseTx = mock(ReadOnlyTransaction.class);
    when(databaseClient.singleUse()).thenReturn(singleUseTx);

    ResultSet queryResult = mock(ResultSet.class);
    when(singleUseTx.executeQuery(any(), any(), any())).thenReturn(queryResult);

    ChangeStreamDao changeStreamDao =
        new ChangeStreamDao(
            CHANGE_STREAM_NAME, databaseClient, rpcPriority, "testjob", Dialect.POSTGRESQL, false);

    // Act
    changeStreamDao.changeStreamQuery(null, null, null, 0L);

    // Assert
    ArgumentCaptor<Statement> captor = ArgumentCaptor.forClass(Statement.class);
    verify(singleUseTx).executeQuery(captor.capture(), any(), any());
    Statement captured = captor.getValue();
    String sql = captured.getSql();
    assertTrue(
        "Expected SQL to contain read_json_", sql.contains("read_json_" + CHANGE_STREAM_NAME));
  }
}
