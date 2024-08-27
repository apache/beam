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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PartitionMetadataAdminDaoTest {

  private DatabaseAdminClient databaseAdminClient;

  private PartitionMetadataAdminDao partitionMetadataAdminDao;

  private PartitionMetadataAdminDao partitionMetadataAdminDaoPostgres;

  private OperationFuture<Void, UpdateDatabaseDdlMetadata> op;

  private ArgumentCaptor<Iterable<String>> statements;

  private static final String INSTANCE_ID = "SPANNER_INSTANCE";

  private static final String DATABASE_ID = "SPANNER_DATABASE";

  private static final String TABLE_NAME = "SPANNER_TABLE";

  private static final int TIMEOUT_MINUTES = 10;

  private static final String TIMED_OUT = "TIMED OUT";

  private static final String INTERRUPTED = "Interrupted";

  @Before
  public void setUp() {
    databaseAdminClient = mock(DatabaseAdminClient.class);
    partitionMetadataAdminDao =
        new PartitionMetadataAdminDao(
            databaseAdminClient, INSTANCE_ID, DATABASE_ID, TABLE_NAME, Dialect.GOOGLE_STANDARD_SQL);
    partitionMetadataAdminDaoPostgres =
        new PartitionMetadataAdminDao(
            databaseAdminClient, INSTANCE_ID, DATABASE_ID, TABLE_NAME, Dialect.POSTGRESQL);
    op = (OperationFuture<Void, UpdateDatabaseDdlMetadata>) mock(OperationFuture.class);
    statements = ArgumentCaptor.forClass(Iterable.class);
    when(databaseAdminClient.updateDatabaseDdl(
            eq(INSTANCE_ID), eq(DATABASE_ID), statements.capture(), isNull()))
        .thenReturn(op);
  }

  @Test
  public void testCreatePartitionMetadataTable() throws Exception {
    when(op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES)).thenReturn(null);
    partitionMetadataAdminDao.createPartitionMetadataTable();
    verify(databaseAdminClient, times(1))
        .updateDatabaseDdl(eq(INSTANCE_ID), eq(DATABASE_ID), statements.capture(), isNull());
    assertEquals(3, ((Collection<?>) statements.getValue()).size());
    Iterator<String> it = statements.getValue().iterator();
    assertTrue(it.next().contains("CREATE TABLE"));
    assertTrue(it.next().contains("CREATE INDEX"));
    assertTrue(it.next().contains("CREATE INDEX"));
  }

  @Test
  public void testCreatePartitionMetadataTablePostgres() throws Exception {
    when(op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES)).thenReturn(null);
    partitionMetadataAdminDaoPostgres.createPartitionMetadataTable();
    verify(databaseAdminClient, times(1))
        .updateDatabaseDdl(eq(INSTANCE_ID), eq(DATABASE_ID), statements.capture(), isNull());
    assertEquals(3, ((Collection<?>) statements.getValue()).size());
    Iterator<String> it = statements.getValue().iterator();
    assertTrue(it.next().contains("CREATE TABLE \""));
    assertTrue(it.next().contains("CREATE INDEX \""));
    assertTrue(it.next().contains("CREATE INDEX \""));
  }

  @Test
  public void testCreatePartitionMetadataTableWithTimeoutException() throws Exception {
    when(op.get(10, TimeUnit.MINUTES)).thenThrow(new TimeoutException(TIMED_OUT));
    try {
      partitionMetadataAdminDao.createPartitionMetadataTable();
      fail();
    } catch (SpannerException e) {
      assertTrue(e.getMessage().contains(TIMED_OUT));
    }
  }

  @Test
  public void testCreatePartitionMetadataTableWithInterruptedException() throws Exception {
    when(op.get(10, TimeUnit.MINUTES)).thenThrow(new InterruptedException(INTERRUPTED));
    try {
      partitionMetadataAdminDao.createPartitionMetadataTable();
      fail();
    } catch (SpannerException e) {
      assertEquals(ErrorCode.CANCELLED, e.getErrorCode());
      assertTrue(e.getMessage().contains(INTERRUPTED));
    }
  }

  @Test
  public void testDeletePartitionMetadataTable() throws Exception {
    when(op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES)).thenReturn(null);
    partitionMetadataAdminDao.deletePartitionMetadataTable();
    verify(databaseAdminClient, times(1))
        .updateDatabaseDdl(eq(INSTANCE_ID), eq(DATABASE_ID), statements.capture(), isNull());
    assertEquals(3, ((Collection<?>) statements.getValue()).size());
    Iterator<String> it = statements.getValue().iterator();
    assertTrue(it.next().contains("DROP INDEX"));
    assertTrue(it.next().contains("DROP INDEX"));
    assertTrue(it.next().contains("DROP TABLE"));
  }

  @Test
  public void testDeletePartitionMetadataTablePostgres() throws Exception {
    when(op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES)).thenReturn(null);
    partitionMetadataAdminDaoPostgres.deletePartitionMetadataTable();
    verify(databaseAdminClient, times(1))
        .updateDatabaseDdl(eq(INSTANCE_ID), eq(DATABASE_ID), statements.capture(), isNull());
    assertEquals(3, ((Collection<?>) statements.getValue()).size());
    Iterator<String> it = statements.getValue().iterator();
    assertTrue(it.next().contains("DROP INDEX \""));
    assertTrue(it.next().contains("DROP INDEX \""));
    assertTrue(it.next().contains("DROP TABLE \""));
  }

  @Test
  public void testDeletePartitionMetadataTableWithTimeoutException() throws Exception {
    when(op.get(10, TimeUnit.MINUTES)).thenThrow(new TimeoutException(TIMED_OUT));
    try {
      partitionMetadataAdminDao.deletePartitionMetadataTable();
      fail();
    } catch (SpannerException e) {
      assertTrue(e.getMessage().contains(TIMED_OUT));
    }
  }

  @Test
  public void testDeletePartitionMetadataTableWithInterruptedException() throws Exception {
    when(op.get(10, TimeUnit.MINUTES)).thenThrow(new InterruptedException(INTERRUPTED));
    try {
      partitionMetadataAdminDao.deletePartitionMetadataTable();
      fail();
    } catch (SpannerException e) {
      assertEquals(ErrorCode.CANCELLED, e.getErrorCode());
      assertTrue(e.getMessage().contains(INTERRUPTED));
    }
  }
}
