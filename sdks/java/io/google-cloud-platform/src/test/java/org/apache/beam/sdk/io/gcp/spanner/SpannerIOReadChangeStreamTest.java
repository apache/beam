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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.MetadataSpannerConfigFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;

@RunWith(Enclosed.class)
public class SpannerIOReadChangeStreamTest {

  private static final String TEST_PROJECT = "my-project";
  private static final String TEST_INSTANCE = "my-instance";
  private static final String TEST_DATABASE = "my-database";
  private static final String TEST_METADATA_INSTANCE = "my-metadata-instance";
  private static final String TEST_METADATA_DATABASE = "my-metadata-database";
  private static final String TEST_METADATA_TABLE = "my-metadata-table";
  private static final String TEST_CHANGE_STREAM = "my-change-stream";

  /** Basic configuration tests using standard JUnit4. */
  @RunWith(JUnit4.class)
  public static class ConfigurationTests {
    @Rule public final transient TestPipeline testPipeline = TestPipeline.create();
    private SpannerConfig spannerConfig;
    private SpannerIO.ReadChangeStream readChangeStream;

    @Before
    public void setUp() {
      spannerConfig =
          SpannerConfig.create()
              .withProjectId(TEST_PROJECT)
              .withInstanceId(TEST_INSTANCE)
              .withDatabaseId(TEST_DATABASE);

      readChangeStream =
          SpannerIO.readChangeStream()
              .withSpannerConfig(spannerConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataInstance(TEST_METADATA_INSTANCE)
              .withMetadataDatabase(TEST_METADATA_DATABASE)
              .withMetadataTable(TEST_METADATA_TABLE)
              .withRpcPriority(RpcPriority.MEDIUM)
              .withInclusiveStartAt(Timestamp.now());
    }

    @Test
    public void testSetPipelineCredential() {
      TestCredential testCredential = new TestCredential();
      // Set the credential in the pipeline options.
      testPipeline.getOptions().as(GcpOptions.class).setGcpCredential(testCredential);
      SpannerConfig changeStreamSpannerConfig = readChangeStream.buildChangeStreamSpannerConfig();
      SpannerConfig metadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, TEST_METADATA_INSTANCE, TEST_METADATA_DATABASE);
      assertNull(changeStreamSpannerConfig.getCredentials());
      assertNull(metadataSpannerConfig.getCredentials());

      SpannerConfig changeStreamSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              changeStreamSpannerConfig, testPipeline.getOptions());
      SpannerConfig metadataSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              metadataSpannerConfig, testPipeline.getOptions());
      assertEquals(testCredential, changeStreamSpannerConfigWithCredential.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfigWithCredential.getCredentials().get());
    }

    @Test
    public void testSetSpannerConfigCredential() {
      TestCredential testCredential = new TestCredential();
      // Set the credential in the SpannerConfig.
      spannerConfig = spannerConfig.withCredentials(testCredential);
      readChangeStream = readChangeStream.withSpannerConfig(spannerConfig);
      SpannerConfig changeStreamSpannerConfig = readChangeStream.buildChangeStreamSpannerConfig();
      SpannerConfig metadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, TEST_METADATA_INSTANCE, TEST_METADATA_DATABASE);
      assertEquals(testCredential, changeStreamSpannerConfig.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfig.getCredentials().get());

      SpannerConfig changeStreamSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              changeStreamSpannerConfig, testPipeline.getOptions());
      SpannerConfig metadataSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              metadataSpannerConfig, testPipeline.getOptions());
      assertEquals(testCredential, changeStreamSpannerConfigWithCredential.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfigWithCredential.getCredentials().get());
    }

    @Test
    public void testWithDefaultCredential() {
      // Get the default credential, without setting any credentials in the pipeline
      // options or SpannerConfig.
      Credentials defaultCredential =
          testPipeline.getOptions().as(GcpOptions.class).getGcpCredential();
      SpannerConfig changeStreamSpannerConfig = readChangeStream.buildChangeStreamSpannerConfig();
      SpannerConfig metadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, TEST_METADATA_INSTANCE, TEST_METADATA_DATABASE);
      assertNull(changeStreamSpannerConfig.getCredentials());
      assertNull(metadataSpannerConfig.getCredentials());

      SpannerConfig changeStreamSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              changeStreamSpannerConfig, testPipeline.getOptions());
      SpannerConfig metadataSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              metadataSpannerConfig, testPipeline.getOptions());
      assertEquals(
          defaultCredential, changeStreamSpannerConfigWithCredential.getCredentials().get());
      assertEquals(defaultCredential, metadataSpannerConfigWithCredential.getCredentials().get());
    }
  }

  /** Parameterized tests for Dialect and Partition Mode combinations. */
  @RunWith(Parameterized.class)
  public static class PartitionModeTests {

    @Parameters(name = "{index}: dialect={0}, mode={1}, expected={2}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][] {
            {Dialect.GOOGLE_STANDARD_SQL, "MUTABLE_KEY_RANGE", true},
            {Dialect.GOOGLE_STANDARD_SQL, "IMMUTABLE_KEY_RANGE", false},
            {Dialect.GOOGLE_STANDARD_SQL, "", false}, // Empty string case
            {Dialect.POSTGRESQL, "MUTABLE_KEY_RANGE", true},
            {Dialect.POSTGRESQL, "IMMUTABLE_KEY_RANGE", false},
            {Dialect.POSTGRESQL, "", false}
          });
    }

    @Parameter(0)
    public Dialect dialect;

    @Parameter(1)
    public String partitionMode;

    @Parameter(2)
    public boolean expected;

    @Test
    public void testIsMutableChangeStream() {
      DatabaseClient databaseClient = mock(DatabaseClient.class);
      ReadOnlyTransaction transaction = mock(ReadOnlyTransaction.class);
      ResultSet resultSet = mock(ResultSet.class);

      when(databaseClient.readOnlyTransaction()).thenReturn(transaction);
      when(transaction.executeQuery(any(Statement.class))).thenReturn(resultSet);

      // Handle the different return values for the mock ResultSet
      if (partitionMode.isEmpty()) {
        // If the partition mode is empty (e.g., the option is not set in Spanner),
        // simulate an empty result set by having next() return false immediately.
        when(resultSet.next()).thenReturn(false); // No row returned
      } else {
        // If a partition mode exists, simulate a result set containing exactly one row.
        // next() returns true for the first call (row exists) and false for the second (end of
        // stream).
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        // When getString(0) is called to retrieve the 'option_value' column from the row,
        // return the specified partitionMode string (e.g., "MUTABLE_KEY_RANGE").
        when(resultSet.getString(0)).thenReturn(partitionMode);
      }

      boolean actual = SpannerIO.isMutableChangeStream(databaseClient, dialect, TEST_CHANGE_STREAM);
      assertEquals(expected, actual);

      // Verify SQL Syntax: Captures the statement to check for dialect-specific
      // syntax
      ArgumentCaptor<Statement> statementCaptor = ArgumentCaptor.forClass(Statement.class);
      verify(transaction).executeQuery(statementCaptor.capture());
      String sql = statementCaptor.getValue().getSql();

      // Ensure the SQL uses the correct parameter placeholder syntax for the given dialect.
      // Different dialects have different requirements for how parameters are bound in queries.
      if (dialect == Dialect.POSTGRESQL) {
        // PostgreSQL-dialect Spanner databases use positional parameters (e.g., $1, $2)
        assertTrue("PostgreSQL SQL should use $1", sql.contains("$1"));
      } else {
        // Google Standard SQL-dialect Spanner databases use named parameters (e.g.,
        // @changeStreamName)
        assertTrue("GoogleSQL SQL should use @changeStreamName", sql.contains("@changeStreamName"));
      }
    }
  }

  /** Tests for error handling and exceptions. */
  @RunWith(JUnit4.class)
  public static class ErrorTests {

    @Test(expected = RuntimeException.class)
    public void testIsMutableChangeStream_PropagatesException() {
      DatabaseClient databaseClient = mock(DatabaseClient.class);
      ReadOnlyTransaction transaction = mock(ReadOnlyTransaction.class);

      // Mock the transaction creation
      when(databaseClient.readOnlyTransaction()).thenReturn(transaction);

      // Simulate a database failure when executing the query
      when(transaction.executeQuery(any(Statement.class)))
          .thenThrow(new RuntimeException("Database connection failed"));

      // The method should log the error and rethrow the exception
      SpannerIO.isMutableChangeStream(databaseClient, Dialect.GOOGLE_STANDARD_SQL, "test-stream");
    }
  }
}
