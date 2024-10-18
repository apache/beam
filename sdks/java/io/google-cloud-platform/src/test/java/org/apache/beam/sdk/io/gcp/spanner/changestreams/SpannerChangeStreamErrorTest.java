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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import static org.apache.beam.sdk.PipelineResult.State.RUNNING;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_CREATED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_END_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_FINISHED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_HEARTBEAT_MILLIS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_PARENT_TOKENS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_RUNNING_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_SCHEDULED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_START_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_WATERMARK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerChangeStreamErrorTest implements Serializable {

  public static final String SPANNER_HOST = "my-host";
  private static final String TEST_PROJECT = "my-project";
  private static final String TEST_INSTANCE = "my-instance";
  private static final String TEST_DATABASE = "my-database";
  private static final String TEST_TABLE = "my-metadata-table";
  private static final String TEST_CHANGE_STREAM = "my-change-stream";

  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  private MockSpannerServiceImpl mockSpannerService;
  private MockServiceHelper serviceHelper;

  @Before
  public void setUp() throws Exception {
    mockSpannerService = new MockSpannerServiceImpl();
    serviceHelper =
        new MockServiceHelper(SPANNER_HOST, Collections.singletonList(mockSpannerService));
    serviceHelper.start();
    serviceHelper.reset();
  }

  @After
  public void tearDown() throws NoSuchFieldException, IllegalAccessException {
    serviceHelper.reset();
    serviceHelper.stop();
    mockSpannerService.reset();
  }

  @Test
  // Error code UNAVAILABLE is retried repeatedly until the RPC times out.
  public void testUnavailableExceptionRetries() throws InterruptedException {
    DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
    options.setBlockOnRun(false);
    options.setRunner(DirectRunner.class);
    Pipeline nonBlockingPipeline = TestPipeline.create(options);

    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.UNAVAILABLE.asRuntimeException()));

    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 1000);
    final Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1);

    try {
      nonBlockingPipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(getSpannerConfig())
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withMetadataTable(TEST_TABLE)
              .withInclusiveStartAt(startTimestamp)
              .withInclusiveEndAt(endTimestamp));
      PipelineResult result = nonBlockingPipeline.run();
      while (result.getState() != RUNNING) {
        Thread.sleep(50);
      }
      // The pipeline continues making requests to Spanner to retry the Unavailable errors.
      assertNull(result.waitUntilFinish(Duration.millis(500)));
    } finally {
      // databaseClient.getDialect does not currently bubble up the correct message.
      // Instead, the error returned is: "DEADLINE_EXCEEDED: Operation did not complete "
      // "in the given time"
      thrown.expectMessage("DEADLINE_EXCEEDED");
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.equalTo(0));
    }
  }

  @Test
  // Error code ABORTED is retried repeatedly until it times out.
  public void testAbortedExceptionRetries() throws InterruptedException {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.ABORTED.asRuntimeException()));

    DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
    options.setBlockOnRun(false);
    options.setRunner(DirectRunner.class);
    Pipeline nonBlockingPipeline = TestPipeline.create(options);

    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 1000);
    final Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1);
    try {
      nonBlockingPipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(getSpannerConfig())
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withMetadataTable(TEST_TABLE)
              .withInclusiveStartAt(startTimestamp)
              .withInclusiveEndAt(endTimestamp));
      PipelineResult result = nonBlockingPipeline.run();
      while (result.getState() != RUNNING) {
        Thread.sleep(50);
      }
      // The pipeline continues making requests to Spanner to retry the Aborted errors.
      assertNull(result.waitUntilFinish(Duration.millis(500)));
    } finally {
      thrown.expectMessage("DEADLINE_EXCEEDED");
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.equalTo(0));
    }
  }

  @Test
  // Error code UNKNOWN is not retried.
  public void testUnknownExceptionDoesNotRetry() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.UNKNOWN.asRuntimeException()));

    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 1000);
    final Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1);
    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(getSpannerConfig())
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withMetadataTable(TEST_TABLE)
              .withInclusiveStartAt(startTimestamp)
              .withInclusiveEndAt(endTimestamp));
      pipeline.run().waitUntilFinish();
    } finally {
      thrown.expect(SpannerException.class);
      thrown.expectMessage("UNKNOWN");
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.equalTo(0));
    }
  }

  @Test
  // Error code RESOURCE_EXHAUSTED is retried repeatedly.
  public void testResourceExhaustedRetry() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));

    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 1000);
    final Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1);

    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(getSpannerConfig())
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withMetadataTable(TEST_TABLE)
              .withInclusiveStartAt(startTimestamp)
              .withInclusiveEndAt(endTimestamp));
      pipeline.run().waitUntilFinish();
    } finally {
      thrown.expectMessage("DEADLINE_EXCEEDED");
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.equalTo(0));
    }
  }

  @Test
  public void testResourceExhaustedRetryWithDefaultSettings() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));

    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 1000);
    final Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1);
    final SpannerConfig changeStreamConfig =
        SpannerConfig.create()
            .withEmulatorHost(StaticValueProvider.of(SPANNER_HOST))
            .withIsLocalChannelProvider(StaticValueProvider.of(true))
            .withCommitRetrySettings(null)
            .withExecuteStreamingSqlRetrySettings(null)
            .withProjectId(TEST_PROJECT)
            .withInstanceId(TEST_INSTANCE)
            .withDatabaseId(TEST_DATABASE);

    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(changeStreamConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withMetadataTable(TEST_TABLE)
              .withInclusiveStartAt(startTimestamp)
              .withInclusiveEndAt(endTimestamp));
      pipeline.run().waitUntilFinish();
    } finally {
      thrown.expect(SpannerException.class);
      thrown.expectMessage("RESOURCE_EXHAUSTED");
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.equalTo(0));
    }
  }

  @Test
  public void testInvalidRecordReceived() {
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 1000);
    final Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1);

    mockGetDialect();
    mockTableExists();
    mockGetWatermark(startTimestamp);
    ResultSet getPartitionResultSet = mockGetParentPartition(startTimestamp, endTimestamp);
    mockGetPartitionsAfter(
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() - 1),
        getPartitionResultSet);
    mockGetPartitionsAfter(
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos()),
        ResultSet.newBuilder().setMetadata(PARTITION_METADATA_RESULT_SET_METADATA).build());
    mockGetPartitionsAfter(
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1),
        ResultSet.newBuilder().setMetadata(PARTITION_METADATA_RESULT_SET_METADATA).build());
    mockInvalidChangeStreamRecordReceived(startTimestamp, endTimestamp);

    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(getSpannerConfig())
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withMetadataTable(TEST_TABLE)
              .withInclusiveStartAt(startTimestamp)
              .withInclusiveEndAt(endTimestamp));
      pipeline.run().waitUntilFinish();
    } finally {
      thrown.expect(SpannerException.class);
      // DatabaseClient.getDialect returns "DEADLINE_EXCEEDED: Operation did not complete in the "
      // given time" even though we mocked it out.
      thrown.expectMessage("DEADLINE_EXCEEDED");
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.equalTo(0));
    }
  }

  @Test
  public void testInvalidRecordReceivedWithDefaultSettings() {
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 1000);
    final Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1);

    mockGetDialect();
    mockTableExists();
    mockGetWatermark(startTimestamp);
    ResultSet getPartitionResultSet = mockGetParentPartition(startTimestamp, endTimestamp);
    mockchangePartitionState(startTimestamp, endTimestamp, "CREATED");
    mockchangePartitionState(startTimestamp, endTimestamp, "SCHEDULED");
    mockGetPartitionsAfter(
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() - 1),
        getPartitionResultSet);
    mockGetPartitionsAfter(
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos()),
        ResultSet.newBuilder().setMetadata(PARTITION_METADATA_RESULT_SET_METADATA).build());
    mockGetPartitionsAfter(
        Timestamp.ofTimeSecondsAndNanos(startTimestamp.getSeconds(), startTimestamp.getNanos() + 1),
        ResultSet.newBuilder().setMetadata(PARTITION_METADATA_RESULT_SET_METADATA).build());
    mockInvalidChangeStreamRecordReceived(startTimestamp, endTimestamp);

    try {
      RetrySettings quickRetrySettings =
          RetrySettings.newBuilder()
              .setInitialRetryDelay(org.threeten.bp.Duration.ofMillis(250))
              .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(1))
              .setRetryDelayMultiplier(5)
              .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(1))
              .build();
      final SpannerConfig changeStreamConfig =
          SpannerConfig.create()
              .withEmulatorHost(StaticValueProvider.of(SPANNER_HOST))
              .withIsLocalChannelProvider(StaticValueProvider.of(true))
              .withCommitRetrySettings(quickRetrySettings)
              .withExecuteStreamingSqlRetrySettings(null)
              .withProjectId(TEST_PROJECT)
              .withInstanceId(TEST_INSTANCE)
              .withDatabaseId(TEST_DATABASE);

      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(changeStreamConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withMetadataTable(TEST_TABLE)
              .withInclusiveStartAt(startTimestamp)
              .withInclusiveEndAt(endTimestamp));
      pipeline.run().waitUntilFinish();
    } finally {
      thrown.expect(PipelineExecutionException.class);
      thrown.expectMessage("Field not found");
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.greaterThan(0));
    }
  }

  private void mockInvalidChangeStreamRecordReceived(Timestamp now, Timestamp after3Seconds) {
    Statement changeStreamQueryStatement =
        Statement.newBuilder(
                "SELECT * FROM READ_my-change-stream(   start_timestamp => @startTimestamp,   end_timestamp => @endTimestamp,   partition_token => @partitionToken,   read_options => null,   heartbeat_milliseconds => @heartbeatMillis)")
            .bind("startTimestamp")
            .to(now)
            .bind("endTimestamp")
            .to(after3Seconds)
            .bind("partitionToken")
            .to((String) null)
            .bind("heartbeatMillis")
            .to(500)
            .build();
    ResultSetMetadata readChangeStreamResultSetMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("COL1")
                            .setType(
                                Type.newBuilder()
                                    .setCode(TypeCode.ARRAY)
                                    .setArrayElementType(
                                        Type.newBuilder()
                                            .setCode(TypeCode.STRUCT)
                                            .setStructType(
                                                StructType.newBuilder()
                                                    .addFields(
                                                        Field.newBuilder()
                                                            .setName("field_name")
                                                            .setType(
                                                                Type.newBuilder()
                                                                    .setCode(TypeCode.STRUCT)
                                                                    .setStructType(
                                                                        StructType.newBuilder()
                                                                            .addFields(
                                                                                Field.newBuilder()
                                                                                    .setType(
                                                                                        Type
                                                                                            .newBuilder()
                                                                                            .setCode(
                                                                                                TypeCode
                                                                                                    .STRING)))))))))))
            .build();
    ResultSet readChangeStreamResultSet =
        ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addValues(
                                        Value.newBuilder()
                                            .setListValue(
                                                ListValue.newBuilder()
                                                    .addValues(
                                                        Value.newBuilder()
                                                            .setListValue(
                                                                ListValue.newBuilder()
                                                                    .addValues(
                                                                        Value.newBuilder()
                                                                            .setStringValue(
                                                                                "bad_value")))))))))
            .setMetadata(readChangeStreamResultSetMetadata)
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(changeStreamQueryStatement, readChangeStreamResultSet));
  }

  private void mockGetPartitionsAfter(Timestamp timestamp, ResultSet getPartitionResultSet) {
    Statement getPartitionsAfterStatement =
        Statement.newBuilder(
                "SELECT * FROM my-metadata-table WHERE CreatedAt > @timestamp ORDER BY CreatedAt ASC, StartTimestamp ASC")
            .bind("timestamp")
            .to(Timestamp.ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos()))
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(getPartitionsAfterStatement, getPartitionResultSet));
  }

  private void mockGetWatermark(Timestamp watermark) {
    Statement watermarkStatement =
        Statement.newBuilder(
                "SELECT Watermark FROM my-metadata-table WHERE State != @state ORDER BY Watermark ASC LIMIT 1")
            .bind("state")
            .to(State.FINISHED.name())
            .build();
    ResultSetMetadata watermarkResultSetMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("Watermark")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .build())
            .build();
    ResultSet watermarkResultSet =
        ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue(watermark.toString()).build())
                    .build())
            .setMetadata(watermarkResultSetMetadata)
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(watermarkStatement, watermarkResultSet));
  }

  private ResultSet mockGetParentPartition(Timestamp startTimestamp, Timestamp after3Seconds) {
    Statement getPartitionStatement =
        Statement.newBuilder("SELECT * FROM my-metadata-table WHERE PartitionToken = @partition")
            .bind("partition")
            .to("Parent0")
            .build();
    ResultSet getPartitionResultSet =
        ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("Parent0"))
                    .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().build()))
                    .addValues(Value.newBuilder().setStringValue(startTimestamp.toString()))
                    .addValues(Value.newBuilder().setStringValue(after3Seconds.toString()))
                    .addValues(Value.newBuilder().setStringValue("500"))
                    .addValues(Value.newBuilder().setStringValue(State.CREATED.name()))
                    .addValues(Value.newBuilder().setStringValue(startTimestamp.toString()))
                    .addValues(Value.newBuilder().setStringValue(startTimestamp.toString()))
                    .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .build())
            .setMetadata(PARTITION_METADATA_RESULT_SET_METADATA)
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(getPartitionStatement, getPartitionResultSet));
    return getPartitionResultSet;
  }

  private void mockTableExists() {
    Statement tableExistsStatement =
        Statement.of(
            "SELECT t.table_name FROM information_schema.tables AS t WHERE t.table_name = 'my-metadata-table'");
    ResultSetMetadata tableExistsResultSetMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("table_name")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    ResultSet tableExistsResultSet =
        ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue(TEST_TABLE).build())
                    .build())
            .setMetadata(tableExistsResultSetMetadata)
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(tableExistsStatement, tableExistsResultSet));
  }

  private ResultSet mockchangePartitionState(
      Timestamp startTimestamp, Timestamp after3Seconds, String state) {
    List<String> tokens = new ArrayList<>();
    tokens.add("Parent0");
    Statement getPartitionStatement =
        Statement.newBuilder(
                "SELECT * FROM my-metadata-table WHERE PartitionToken IN UNNEST(@partitionTokens) AND State = @state")
            .bind("partitionTokens")
            .toStringArray(tokens)
            .bind("state")
            .to(state)
            .build();
    ResultSet getPartitionResultSet =
        ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("Parent0"))
                    .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().build()))
                    .addValues(Value.newBuilder().setStringValue(startTimestamp.toString()))
                    .addValues(Value.newBuilder().setStringValue(after3Seconds.toString()))
                    .addValues(Value.newBuilder().setStringValue("500"))
                    .addValues(Value.newBuilder().setStringValue(State.CREATED.name()))
                    .addValues(Value.newBuilder().setStringValue(startTimestamp.toString()))
                    .addValues(Value.newBuilder().setStringValue(startTimestamp.toString()))
                    .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                    .build())
            .setMetadata(PARTITION_METADATA_RESULT_SET_METADATA)
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(getPartitionStatement, getPartitionResultSet));
    return getPartitionResultSet;
  }

  private void mockGetDialect() {
    Statement determineDialectStatement =
        Statement.newBuilder(
                "SELECT 'POSTGRESQL' AS DIALECT\n"
                    + "FROM INFORMATION_SCHEMA.SCHEMATA\n"
                    + "WHERE SCHEMA_NAME='information_schema'\n"
                    + "UNION ALL\n"
                    + "SELECT 'GOOGLE_STANDARD_SQL' AS DIALECT\n"
                    + "FROM INFORMATION_SCHEMA.SCHEMATA\n"
                    + "WHERE SCHEMA_NAME='INFORMATION_SCHEMA' AND CATALOG_NAME=''")
            .build();
    ResultSetMetadata dialectResultSetMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("dialect")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    ResultSet dialectResultSet =
        ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("GOOGLE_STANDARD_SQL").build())
                    .build())
            .setMetadata(dialectResultSetMetadata)
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(determineDialectStatement, dialectResultSet));
  }

  private SpannerConfig getSpannerConfig() {
    RetrySettings quickRetrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelay(org.threeten.bp.Duration.ofMillis(250))
            .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(1))
            .setRetryDelayMultiplier(5)
            .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(1))
            .build();
    return SpannerConfig.create()
        .withEmulatorHost(StaticValueProvider.of(SPANNER_HOST))
        .withIsLocalChannelProvider(StaticValueProvider.of(true))
        .withCommitRetrySettings(quickRetrySettings)
        .withExecuteStreamingSqlRetrySettings(quickRetrySettings)
        .withProjectId(TEST_PROJECT)
        .withInstanceId(TEST_INSTANCE)
        .withDatabaseId(TEST_DATABASE);
  }

  private static final ResultSetMetadata PARTITION_METADATA_RESULT_SET_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_PARTITION_TOKEN)
                          .setType(Type.newBuilder().setCode(TypeCode.STRING))
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_PARENT_TOKENS)
                          .setType(
                              Type.newBuilder()
                                  .setCode(TypeCode.ARRAY)
                                  .setArrayElementType(Type.newBuilder().setCode(TypeCode.STRING)))
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_START_TIMESTAMP)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_END_TIMESTAMP)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_HEARTBEAT_MILLIS)
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_STATE)
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_WATERMARK)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_CREATED_AT)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_SCHEDULED_AT)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_RUNNING_AT)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP)))
                  .addFields(
                      Field.newBuilder()
                          .setName(COLUMN_FINISHED_AT)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP)))
                  .build())
          .build();
}
