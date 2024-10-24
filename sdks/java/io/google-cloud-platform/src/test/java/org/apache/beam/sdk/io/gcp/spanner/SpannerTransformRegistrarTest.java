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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.TimestampBound;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.spanner.SpannerTransformRegistrar.InsertBuilder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerTransformRegistrar.ReadBuilder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Before;
import org.junit.Test;

public class SpannerTransformRegistrarTest {

  public static final String SPANNER_INSTANCE = "spanner-instance";
  public static final String SPANNER_DATABASE = "spanner-database";
  public static final String SPANNER_PROJECT = "spanner-project";
  public static final String SPANNER_TABLE = "spanner-table";
  public static final String SPANNER_SQL_QUERY = "SELECT * from spanner_table;";
  private SpannerTransformRegistrar spannerTransformRegistrar;
  private ReadBuilder readBuilder;
  private InsertBuilder writeBuilder;

  @Before
  public void setup() {
    spannerTransformRegistrar = new SpannerTransformRegistrar();
    readBuilder = new ReadBuilder();
    writeBuilder = new InsertBuilder();
  }

  @Test
  public void testKnownBuilderInstances() {
    Map<String, ExternalTransformBuilder<?, ?, ?>> builderInstancesMap =
        spannerTransformRegistrar.knownBuilderInstances();
    assertEquals(6, builderInstancesMap.size());
    assertThat(builderInstancesMap, IsMapContaining.hasKey(SpannerTransformRegistrar.INSERT_URN));
    assertThat(builderInstancesMap, IsMapContaining.hasKey(SpannerTransformRegistrar.UPDATE_URN));
    assertThat(builderInstancesMap, IsMapContaining.hasKey(SpannerTransformRegistrar.REPLACE_URN));
    assertThat(
        builderInstancesMap,
        IsMapContaining.hasKey(SpannerTransformRegistrar.INSERT_OR_UPDATE_URN));
    assertThat(builderInstancesMap, IsMapContaining.hasKey(SpannerTransformRegistrar.DELETE_URN));
    assertThat(builderInstancesMap, IsMapContaining.hasKey(SpannerTransformRegistrar.READ_URN));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReadBuilderBuildExternalWithMissingProjectId() {
    readBuilder.buildExternal(new ReadBuilder.Configuration());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReadBuilderBuildExternalWithMissingDatabaseId() {
    ReadBuilder.Configuration configuration = new ReadBuilder.Configuration();
    configuration.setProjectId(SPANNER_PROJECT);
    readBuilder.buildExternal(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReadBuilderBuildExternalWithMissingInstanceId() {
    ReadBuilder.Configuration configuration = new ReadBuilder.Configuration();
    configuration.setProjectId(SPANNER_PROJECT);
    configuration.setDatabaseId(SPANNER_DATABASE);
    readBuilder.buildExternal(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReadBuilderBuildExternalWithMissingSchema() {
    ReadBuilder.Configuration configuration = getBasicReadConfiguration();
    readBuilder.buildExternal(configuration);
  }

  @Test
  public void testReadBuilderBuildExternalWithSpannerTable() throws InvalidProtocolBufferException {
    ReadBuilder.Configuration configuration = getBasicReadConfiguration();
    configuration.setTable(SPANNER_TABLE);
    configuration.setSchema(getBasicSchemaBytes());
    PTransform<PBegin, PCollection<Row>> readTransform = readBuilder.buildExternal(configuration);
    assertNotNull(readTransform);
  }

  @Test
  public void testReadBuilderBuildExternalWithSqlQuery() throws InvalidProtocolBufferException {
    ReadBuilder.Configuration configuration = getBasicReadConfiguration();
    configuration.setSql(SPANNER_SQL_QUERY);
    configuration.setSchema(getBasicSchemaBytes());
    PTransform<PBegin, PCollection<Row>> readTransform = readBuilder.buildExternal(configuration);
    assertNotNull(readTransform);
  }

  @Test(expected = IllegalStateException.class)
  public void testReadBuilderBuildExternalWithBothSqlQueryAndSpannerTable()
      throws InvalidProtocolBufferException {
    ReadBuilder.Configuration configuration = getBasicReadConfiguration();
    configuration.setSql(SPANNER_SQL_QUERY);
    configuration.setTable(SPANNER_TABLE);
    configuration.setSchema(getBasicSchemaBytes());
    readBuilder.buildExternal(configuration);
  }

  @Test
  public void testReadBuilderBuildExternalWithTimestampBoundMode()
      throws InvalidProtocolBufferException {
    ReadBuilder.Configuration configuration = getBasicReadConfiguration();
    configuration.setSql(SPANNER_SQL_QUERY);
    configuration.setSchema(getBasicSchemaBytes());

    Stream.of(TimestampBound.Mode.values())
        .forEach(
            mode -> {
              configuration.setTimestampBoundMode(mode.toString());
              configuration.setStaleness(0L);
              configuration.setTimeUnit(TimeUnit.MILLISECONDS.toString());
              configuration.setReadTimestamp(
                  Timestamp.parseTimestamp("2077-10-15T00:00:00").toString());
              PTransform<PBegin, PCollection<Row>> readTransform =
                  readBuilder.buildExternal(configuration);
              assertNotNull(readTransform);
            });
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriteBuilderBuildExternalWithMissingProjectId() {
    writeBuilder.buildExternal(new InsertBuilder.Configuration());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriteBuilderBuildExternalWithMissingDatabaseId() {
    InsertBuilder.Configuration configuration = new InsertBuilder.Configuration();
    configuration.setProjectId(SPANNER_PROJECT);
    writeBuilder.buildExternal(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriteBuilderBuildExternalWithMissingInstanceId() {
    InsertBuilder.Configuration configuration = new InsertBuilder.Configuration();
    configuration.setProjectId(SPANNER_PROJECT);
    configuration.setDatabaseId(SPANNER_DATABASE);
    writeBuilder.buildExternal(configuration);
  }

  @Test
  public void testWriteBuilderBuildExternal() {
    InsertBuilder.Configuration configuration = getBasicWriteConfiguration();
    PTransform<PCollection<Row>, PDone> writeTransform = writeBuilder.buildExternal(configuration);
    assertNotNull(writeTransform);
  }

  private byte[] getBasicSchemaBytes() {
    return SchemaTranslation.schemaToProto(getBasicSchema(), true).toByteArray();
  }

  private Schema getBasicSchema() {
    return Schema.builder().addStringField("configField").build();
  }

  private ReadBuilder.Configuration getBasicReadConfiguration() {
    ReadBuilder.Configuration configuration = new ReadBuilder.Configuration();
    configuration.setProjectId(SPANNER_PROJECT);
    configuration.setDatabaseId(SPANNER_DATABASE);
    configuration.setInstanceId(SPANNER_INSTANCE);
    return configuration;
  }

  private InsertBuilder.Configuration getBasicWriteConfiguration() {
    InsertBuilder.Configuration configuration = new InsertBuilder.Configuration();
    configuration.setProjectId(SPANNER_PROJECT);
    configuration.setDatabaseId(SPANNER_DATABASE);
    configuration.setInstanceId(SPANNER_INSTANCE);
    configuration.setMaxBatchSizeBytes(100L);
    configuration.setMaxNumberMutations(100L);
    configuration.setMaxNumberRows(100L);
    configuration.setGroupingFactor(100L);
    configuration.setHost("spanner-host");
    configuration.setEmulatorHost("spanner-emulator-host");
    configuration.setCommitDeadline(100L);
    configuration.setMaxCumulativeBackoff(100L);
    return configuration;
  }
}
