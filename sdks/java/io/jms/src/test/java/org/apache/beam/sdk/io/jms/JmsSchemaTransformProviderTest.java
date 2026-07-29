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
package org.apache.beam.sdk.io.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.jms.JmsReadSchemaTransformProvider.ReadConfiguration;
import org.apache.beam.sdk.io.jms.JmsWriteSchemaTransformProvider.WriteConfiguration;
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

/** Tests for {@link JmsReadSchemaTransformProvider} and {@link JmsWriteSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class JmsSchemaTransformProviderTest {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testReadFindTransform() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == JmsReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider jmsProvider = providers.get(0);

    assertEquals(Lists.newArrayList("output"), jmsProvider.outputCollectionNames());
    assertEquals(Lists.newArrayList(), jmsProvider.inputCollectionNames());
    assertEquals("beam:schematransform:org.apache.beam:jms_read:v1", jmsProvider.identifier());
    assertNotNull(jmsProvider.description());

    assertEquals(
        Sets.newHashSet(
            "connection_configuration",
            "queue",
            "topic",
            "max_num_records",
            "max_read_time_seconds",
            "close_timeout_seconds",
            "acknowledge_mode",
            "individual_acknowledge_mode_code"),
        jmsProvider.configurationSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testReadBuildTransformWithQueue() {
    ReadConfiguration readConfig =
        ReadConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setQueue("TEST_QUEUE")
            .setMaxNumRecords(100L)
            .setMaxReadTimeSeconds(5L)
            .setCloseTimeoutSeconds(10L)
            .setAcknowledgeMode("CLIENT_ACKNOWLEDGE")
            .setIndividualAcknowledgeModeCode(101)
            .build();

    SchemaTransform transform = new JmsReadSchemaTransformProvider().from(readConfig);
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.empty(pipeline));

    assertEquals(1, output.getAll().size());
    assertTrue(output.has("output"));
    assertEquals(
        Schema.builder().addStringField("payload").build(), output.get("output").getSchema());
  }

  @Test
  public void testReadBuildTransformWithTopic() {
    ReadConfiguration readConfig =
        ReadConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setTopic("TEST_TOPIC")
            .build();

    SchemaTransform transform = new JmsReadSchemaTransformProvider().from(readConfig);
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.empty(pipeline));

    assertEquals(1, output.getAll().size());
    assertTrue(output.has("output"));
    assertEquals(
        Schema.builder().addStringField("payload").build(), output.get("output").getSchema());
  }

  @Test
  public void testReadInvalidConfigurations() {
    ReadConfiguration bothConfig =
        ReadConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setQueue("TEST_QUEUE")
            .setTopic("TEST_TOPIC")
            .build();
    SchemaTransform bothTransform = new JmsReadSchemaTransformProvider().from(bothConfig);
    assertThrows(
        IllegalArgumentException.class,
        () -> bothTransform.expand(PCollectionRowTuple.empty(pipeline)));

    ReadConfiguration neitherConfig =
        ReadConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .build();
    SchemaTransform neitherTransform = new JmsReadSchemaTransformProvider().from(neitherConfig);
    assertThrows(
        IllegalArgumentException.class,
        () -> neitherTransform.expand(PCollectionRowTuple.empty(pipeline)));
  }

  @Test
  public void testReadWithNonEmptyInputThrows() {
    ReadConfiguration readConfig =
        ReadConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setQueue("TEST_QUEUE")
            .build();
    SchemaTransform transform = new JmsReadSchemaTransformProvider().from(readConfig);

    PCollection<Row> dummyInput =
        pipeline.apply(
            "CreateDummy", Create.empty(Schema.builder().addStringField("dummy").build()));
    assertThrows(
        IllegalStateException.class,
        () -> transform.expand(PCollectionRowTuple.of("input", dummyInput)));
  }

  @Test
  public void testWriteFindTransform() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == JmsWriteSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider jmsProvider = providers.get(0);

    assertEquals(Lists.newArrayList(), jmsProvider.outputCollectionNames());
    assertEquals(Lists.newArrayList("input"), jmsProvider.inputCollectionNames());
    assertEquals("beam:schematransform:org.apache.beam:jms_write:v1", jmsProvider.identifier());
    assertNotNull(jmsProvider.description());

    assertEquals(
        Sets.newHashSet("connection_configuration", "queue", "topic"),
        jmsProvider.configurationSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testWriteBuildTransformWithQueueAndTopic() {
    WriteConfiguration queueConfig =
        WriteConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setQueue("TEST_QUEUE")
            .build();
    SchemaTransform queueTransform = new JmsWriteSchemaTransformProvider().from(queueConfig);
    Schema schema = Schema.builder().addStringField("payload").build();
    PCollection<Row> inputRows = pipeline.apply("CreateQueueRows", Create.empty(schema));
    PCollectionRowTuple output = queueTransform.expand(PCollectionRowTuple.of("input", inputRows));
    assertTrue(output.getAll().isEmpty());

    WriteConfiguration topicConfig =
        WriteConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setTopic("TEST_TOPIC")
            .build();
    SchemaTransform topicTransform = new JmsWriteSchemaTransformProvider().from(topicConfig);
    Schema bytesSchema = Schema.builder().addByteArrayField("bytes").build();
    PCollection<Row> bytesRows = pipeline.apply("CreateTopicRows", Create.empty(bytesSchema));
    PCollectionRowTuple topicOutput =
        topicTransform.expand(PCollectionRowTuple.of("input", bytesRows));
    assertTrue(topicOutput.getAll().isEmpty());
  }

  @Test
  public void testWriteInvalidConfigurations() {
    WriteConfiguration bothConfig =
        WriteConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setQueue("TEST_QUEUE")
            .setTopic("TEST_TOPIC")
            .build();
    SchemaTransform bothTransform = new JmsWriteSchemaTransformProvider().from(bothConfig);
    Schema schema = Schema.builder().addStringField("payload").build();
    PCollection<Row> inputRows = pipeline.apply("CreateBothRows", Create.empty(schema));
    assertThrows(
        IllegalArgumentException.class,
        () -> bothTransform.expand(PCollectionRowTuple.of("input", inputRows)));

    WriteConfiguration neitherConfig =
        WriteConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .build();
    SchemaTransform neitherTransform = new JmsWriteSchemaTransformProvider().from(neitherConfig);
    PCollection<Row> inputRows2 = pipeline.apply("CreateNeitherRows", Create.empty(schema));
    assertThrows(
        IllegalArgumentException.class,
        () -> neitherTransform.expand(PCollectionRowTuple.of("input", inputRows2)));
  }

  @Test
  public void testWriteInvalidInputSchema() {
    WriteConfiguration config =
        WriteConfiguration.builder()
            .setConnectionConfiguration(
                JmsIO.ConnectionConfiguration.create("tcp://localhost:61616"))
            .setQueue("TEST_QUEUE")
            .build();
    SchemaTransform transform = new JmsWriteSchemaTransformProvider().from(config);

    Schema invalidSchema = Schema.builder().addInt32Field("id").addStringField("name").build();
    PCollection<Row> inputRows =
        pipeline.apply("CreateInvalidSchemaRows", Create.empty(invalidSchema));
    assertThrows(
        IllegalStateException.class,
        () -> transform.expand(PCollectionRowTuple.of("input", inputRows)));
  }
}
