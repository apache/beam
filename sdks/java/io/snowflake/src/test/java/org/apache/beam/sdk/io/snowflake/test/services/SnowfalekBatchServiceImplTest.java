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
package org.apache.beam.sdk.io.snowflake.test.services;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.sql.*;
import java.util.Arrays;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.FileFormat;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeBatchServiceConfig;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeBatchServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SnowfalekBatchServiceImplTest {
  @Mock DataSource mockDataSource;
  @Mock Connection mockConnection;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
  }

  @Test
  public void testReadFromQuery() throws Exception {
    when(mockDataSource.getConnection().prepareStatement(any()))
        .thenReturn(mock(PreparedStatement.class));

    SnowflakeBatchServiceImpl service = new SnowflakeBatchServiceImpl();
    String result =
        service.read(
            new SnowflakeBatchServiceConfig(
                /*dataSourceProviderFn*/ (unused) -> mockDataSource,
                /* database */ "TEST-DATABASE",
                /* schema */ "TEST-SCHEMA",
                /* table */ null,
                /* query */ "TEST-QUERY",
                /* storageIntegrationName */ "TEST-STORAGE-INTEGRATION",
                /* stagingBucketDir */ "gcs://TEST-BUCKET/",
                /* quotationMark */ "'"));
    verify(mockConnection)
        .prepareStatement(
            "COPY INTO 'gcs://TEST-BUCKET/' FROM (TEST-QUERY) STORAGE_INTEGRATION=TEST-STORAGE-INTEGRATION FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_OPTIONALLY_ENCLOSED_BY='0x27');");

    assertEquals("gcs://TEST-BUCKET/*", result);
  }

  @Test
  public void testWriteCsvWithCreateCreationAndEmptyCheck() throws Exception {
    SnowflakeBatchServiceImpl service = new SnowflakeBatchServiceImpl();
    when(mockDataSource.getConnection().prepareStatement(any()))
        .thenAnswer(
            (args) -> {
              String sql = args.getArgument(0);
              if ("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'TEST-TABLE');"
                  .equals(sql)) {
                PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
                when(mockPreparedStatement.executeQuery())
                    .then(
                        (args2) -> {
                          ResultSet mockResultSet = mock(ResultSet.class);
                          when(mockResultSet.next()).thenReturn(true);
                          when(mockResultSet.getBoolean(1)).thenReturn(true);
                          return mockResultSet;
                        });
                return mockPreparedStatement;
              } else if ("CREATE TABLE TEST-TABLE (id VARCHAR);".equals(sql)) {
                return mock(PreparedStatement.class);
              } else if ("SELECT count(*) FROM TEST-TABLE LIMIT 1;".equals(sql)) {
                PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
                when(mockPreparedStatement.executeQuery())
                    .then(
                        (args2) -> {
                          ResultSet mockResultSet = mock(ResultSet.class);
                          when(mockResultSet.next()).thenReturn(true);
                          when(mockResultSet.getInt(1)).thenReturn(0);
                          return mockResultSet;
                        });
                return mockPreparedStatement;
              } else if ("COPY INTO TEST-DATABASE.TEST-SCHEMA.TEST-TABLE FROM (TEST-QUERY) FILES=('TEST-FILE1', 'TEST-FILE2') FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='0x41' COMPRESSION=GZIP) STORAGE_INTEGRATION=TEST-STORAGE-INTEGRATION;"
                  .equals(sql)) {
                return mock(PreparedStatement.class);
              }
              throw new IllegalArgumentException("Unknown sql statement: " + sql);
            });

    service.write(
        new SnowflakeBatchServiceConfig(
            /* dataSourceProviderFn */ (unused) -> mockDataSource,
            /* filesList */ Arrays.asList("TEST-FILE1", "TEST-FILE2"),
            /* tableSchema */ new SnowflakeTableSchema(
                SnowflakeColumn.of("id", SnowflakeVarchar.of())),
            /* database */ "TEST-DATABASE",
            /* schema */ "TEST-SCHEMA",
            /* table */ "TEST-TABLE",
            /* query */ "TEST-QUERY",
            /* createDisposition */ CreateDisposition.CREATE_IF_NEEDED,
            /* writeDisposition */ WriteDisposition.EMPTY,
            /* storageIntegrationName */ "TEST-STORAGE-INTEGRATION",
            /* stagingBucketDir */ "TEST-BUCKET",
            /* quotationMark */ "A",
            /* fileFormat */ FileFormat.CSV));
  }

  @Test
  public void testWriteJson() throws Exception {
    SnowflakeBatchServiceImpl service = new SnowflakeBatchServiceImpl();
    when(mockDataSource.getConnection().prepareStatement(any()))
        .thenAnswer(
            (args) -> {
              String sql = args.getArgument(0);
              if ("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'TEST-TABLE');"
                  .equals(sql)) {
                PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
                when(mockPreparedStatement.executeQuery())
                    .then(
                        (args2) -> {
                          ResultSet mockResultSet = mock(ResultSet.class);
                          when(mockResultSet.next()).thenReturn(true);
                          when(mockResultSet.getBoolean(1)).thenReturn(true);
                          return mockResultSet;
                        });
                return mockPreparedStatement;
              } else if ("COPY INTO TEST-DATABASE.TEST-SCHEMA.TEST-TABLE FROM (TEST-QUERY) FILES=('TEST-FILE1', 'TEST-FILE2') FILE_FORMAT=(TYPE=JSON) STORAGE_INTEGRATION=TEST-STORAGE-INTEGRATION;"
                  .equals(sql)) {
                return mock(PreparedStatement.class);
              }
              throw new IllegalArgumentException("Unknown sql statement: " + sql);
            });

    service.write(
        new SnowflakeBatchServiceConfig(
            /* dataSourceProviderFn */ (unused) -> mockDataSource,
            /* filesList */ Arrays.asList("TEST-FILE1", "TEST-FILE2"),
            /* tableSchema */ new SnowflakeTableSchema(
                SnowflakeColumn.of("id", SnowflakeVarchar.of())),
            /* database */ "TEST-DATABASE",
            /* schema */ "TEST-SCHEMA",
            /* table */ "TEST-TABLE",
            /* query */ "TEST-QUERY",
            /* createDisposition */ CreateDisposition.CREATE_NEVER,
            /* writeDisposition */ WriteDisposition.APPEND,
            /* storageIntegrationName */ "TEST-STORAGE-INTEGRATION",
            /* stagingBucketDir */ "TEST-BUCKET",
            /* quotationMark */ "A",
            /* fileFormat */ FileFormat.JSON));
  }
}
