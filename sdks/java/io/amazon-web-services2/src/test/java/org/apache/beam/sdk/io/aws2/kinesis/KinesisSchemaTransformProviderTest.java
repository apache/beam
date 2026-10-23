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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReadSchemaTransformProvider.KinesisReadSchemaTransformConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisWriteSchemaTransformProvider.KinesisWriteSchemaTransformConfiguration;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link KinesisReadSchemaTransformProvider} and {@link
 * KinesisWriteSchemaTransformProvider}.
 */
@RunWith(JUnit4.class)
public class KinesisSchemaTransformProviderTest {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.fromOptions(PipelineOptionsFactory.create())
          .enableAbandonedNodeEnforcement(false);

  @Test
  public void testReadFindTransform() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KinesisReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider provider = providers.get(0);

    assertEquals(Lists.newArrayList("output"), provider.outputCollectionNames());
    assertEquals(Lists.newArrayList(), provider.inputCollectionNames());
    assertEquals("beam:schematransform:org.apache.beam:kinesis_read:v1", provider.identifier());
    assertNotNull(provider.description());

    assertEquals(
        Sets.newHashSet(
            "stream_name",
            "aws_access_key",
            "aws_secret_key",
            "region",
            "service_endpoint",
            "verify_certificate",
            "max_num_records",
            "max_read_time",
            "initial_position_in_stream",
            "initial_timestamp_in_stream",
            "request_records_limit",
            "up_to_date_threshold",
            "max_capacity_per_shard",
            "watermark_policy",
            "watermark_idle_duration_threshold",
            "rate_limit"),
        provider.configurationSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testReadBuildTransform() {
    KinesisReadSchemaTransformConfiguration readConfig =
        KinesisReadSchemaTransformConfiguration.builder()
            .setStreamName("test-stream")
            .setAwsAccessKey("access")
            .setAwsSecretKey("secret")
            .setRegion("us-east-1")
            .setMaxNumRecords(100L)
            .setMaxReadTime(5000L)
            .setInitialPositionInStream("LATEST")
            .build();

    SchemaTransform transform = new KinesisReadSchemaTransformProvider().from(readConfig);
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.empty(pipeline));

    assertEquals(1, output.getAll().size());
    assertTrue(output.has("output"));
    assertEquals(
        KinesisReadSchemaTransformProvider.OUTPUT_SCHEMA, output.get("output").getSchema());
  }

  @Test
  public void testReadWithNonEmptyInputThrows() {
    KinesisReadSchemaTransformConfiguration readConfig =
        KinesisReadSchemaTransformConfiguration.builder()
            .setStreamName("test-stream")
            .setAwsAccessKey("access")
            .setAwsSecretKey("secret")
            .setRegion("us-east-1")
            .build();
    SchemaTransform transform = new KinesisReadSchemaTransformProvider().from(readConfig);

    PCollection<Row> dummyInput =
        pipeline.apply(
            "CreateDummy", Create.empty(Schema.builder().addStringField("dummy").build()));
    assertThrows(
        IllegalStateException.class,
        () -> transform.expand(PCollectionRowTuple.of("input", dummyInput)));
  }

  @Test
  public void testReadInvalidWatermarkPolicyThrows() {
    KinesisReadSchemaTransformConfiguration readConfig =
        KinesisReadSchemaTransformConfiguration.builder()
            .setStreamName("test-stream")
            .setAwsAccessKey("access")
            .setAwsSecretKey("secret")
            .setRegion("us-east-1")
            .setWatermarkPolicy("INVALID")
            .build();
    SchemaTransform transform = new KinesisReadSchemaTransformProvider().from(readConfig);
    assertThrows(
        IllegalArgumentException.class,
        () -> transform.expand(PCollectionRowTuple.empty(pipeline)));
  }

  @Test
  public void testWriteFindTransform() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KinesisWriteSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider provider = providers.get(0);

    assertEquals(Lists.newArrayList(), provider.outputCollectionNames());
    assertEquals(Lists.newArrayList("input"), provider.inputCollectionNames());
    assertEquals("beam:schematransform:org.apache.beam:kinesis_write:v1", provider.identifier());
    assertNotNull(provider.description());

    assertEquals(
        Sets.newHashSet(
            "stream_name",
            "aws_access_key",
            "aws_secret_key",
            "region",
            "partition_key",
            "service_endpoint",
            "verify_certificate",
            "aggregation_enabled",
            "aggregation_max_bytes",
            "aggregation_max_buffered_time",
            "aggregation_shard_refresh_interval"),
        provider.configurationSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testWriteBuildTransform() {
    KinesisWriteSchemaTransformConfiguration writeConfig =
        KinesisWriteSchemaTransformConfiguration.builder()
            .setStreamName("test-stream")
            .setAwsAccessKey("access")
            .setAwsSecretKey("secret")
            .setRegion("us-east-1")
            .setPartitionKey("pk")
            .build();
    SchemaTransform transform = new KinesisWriteSchemaTransformProvider().from(writeConfig);
    Schema schema = Schema.builder().addByteArrayField("data").build();
    PCollection<Row> inputRows = pipeline.apply("CreateRows", Create.empty(schema));
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.of("input", inputRows));
    assertTrue(output.getAll().isEmpty());
  }

  @Test
  public void testWriteInvalidSchemaThrows() {
    KinesisWriteSchemaTransformConfiguration writeConfig =
        KinesisWriteSchemaTransformConfiguration.builder()
            .setStreamName("test-stream")
            .setAwsAccessKey("access")
            .setAwsSecretKey("secret")
            .setRegion("us-east-1")
            .setPartitionKey("pk")
            .build();
    SchemaTransform transform = new KinesisWriteSchemaTransformProvider().from(writeConfig);
    Schema schema = Schema.builder().addStringField("foo").addStringField("bar").build();
    PCollection<Row> inputRows = pipeline.apply("CreateInvalidRows", Create.empty(schema));
    assertThrows(
        IllegalArgumentException.class,
        () -> transform.expand(PCollectionRowTuple.of("input", inputRows)));
  }
}
