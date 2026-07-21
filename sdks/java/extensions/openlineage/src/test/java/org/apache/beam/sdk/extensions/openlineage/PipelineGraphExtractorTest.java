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
package org.apache.beam.sdk.extensions.openlineage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link VisitorFactory} and the per-IO {@link PipelineLineageVisitor}s. */
@RunWith(JUnit4.class)
public class PipelineGraphExtractorTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Schema BEAM_SCHEMA =
      Schema.builder().addInt64Field("id").addStringField("name").build();

  @Test
  public void testPubsubReadTopicExtraction() {
    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
    pipeline.apply(PubsubIO.readStrings().fromTopic("projects/acme-prod/topics/orders-events"));

    List<DatasetIdentifier> inputs = extract(pipeline, true);
    assertEquals(1, inputs.size());
    assertEquals("pubsub", inputs.get(0).getNamespace());
    assertEquals("topic:acme-prod:orders-events", inputs.get(0).getName());
  }

  @Test
  public void testPubsubReadSubscriptionExtraction() {
    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
    pipeline.apply(
        PubsubIO.readStrings().fromSubscription("projects/acme-prod/subscriptions/orders-sub"));

    List<DatasetIdentifier> inputs = extract(pipeline, true);
    assertEquals(1, inputs.size());
    assertEquals("pubsub", inputs.get(0).getNamespace());
    assertEquals("subscription:acme-prod:orders-sub", inputs.get(0).getName());
  }

  @Test
  public void testIcebergWriteWithReachableCatalogUsesTableLocation() throws Exception {
    String warehouse = "file://" + temporaryFolder.newFolder("warehouse").getAbsolutePath();
    TableIdentifier tableId = TableIdentifier.parse("demo.orders");
    try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse)) {
      catalog.createTable(
          tableId,
          new org.apache.iceberg.Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "name", Types.StringType.get())));
    }

    List<DatasetIdentifier> outputs =
        extract(icebergWritePipeline(warehouse, "hadoop", null), false);
    assertEquals(1, outputs.size());
    DatasetIdentifier identifier = outputs.get(0);
    // Primary identity is the physical table location, per the Spark integration's
    // IcebergHandler; local file locations map to the spec's "file" namespace.
    assertEquals("file", identifier.getNamespace());
    assertTrue(identifier.getName().endsWith("/warehouse/demo/orders"));
    // The catalog table identity rides in a TABLE symlink.
    assertEquals(1, identifier.getSymlinks().size());
    assertEquals("demo.orders", identifier.getSymlinks().get(0).getName());
    assertEquals(DatasetIdentifier.SymlinkType.TABLE, identifier.getSymlinks().get(0).getType());
  }

  @Test
  public void testIcebergWriteWithUnreachableRestCatalogFallsBack() {
    // Nothing listens on this port; extraction must degrade gracefully, not fail submission.
    List<DatasetIdentifier> outputs =
        extract(
            icebergWritePipeline(
                "gs://acme-lakehouse/warehouse", "rest", "http://127.0.0.1:1/catalog"),
            false);
    assertEquals(1, outputs.size());
    DatasetIdentifier identifier = outputs.get(0);
    assertEquals("iceberg://127.0.0.1:1", identifier.getNamespace());
    assertEquals("demo.orders", identifier.getName());
    assertEquals(1, identifier.getSymlinks().size());
    assertEquals("http://127.0.0.1:1/catalog", identifier.getSymlinks().get(0).getNamespace());
  }

  @Test
  public void testPubsubWriteTopicExtraction() {
    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
    pipeline
        .apply(org.apache.beam.sdk.transforms.Create.of("a"))
        .apply(PubsubIO.writeStrings().to("projects/acme-prod/topics/enriched-orders"));

    List<DatasetIdentifier> outputs = extract(pipeline, false);
    assertEquals(1, outputs.size());
    assertEquals("pubsub", outputs.get(0).getNamespace());
    assertEquals("topic:acme-prod:enriched-orders", outputs.get(0).getName());
  }

  @Test
  public void testLineageProviderTransformIsPickedUp() {
    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
    pipeline.apply(new ProviderTransform());

    List<DatasetIdentifier> inputs = extract(pipeline, true);
    assertEquals(1, inputs.size());
    assertEquals("kafka://broker:9092", inputs.get(0).getNamespace());
    assertEquals("events", inputs.get(0).getName());
  }

  private static Pipeline icebergWritePipeline(String warehouse, String type, String uri) {
    Map<String, String> catalogProps = new HashMap<>();
    catalogProps.put("type", type);
    catalogProps.put("warehouse", warehouse);
    if (uri != null) {
      catalogProps.put("uri", uri);
    }
    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
    pipeline
        .apply(
            org.apache.beam.sdk.transforms.Create.of(
                    Row.withSchema(BEAM_SCHEMA).addValues(1L, "a").build())
                .withRowSchema(BEAM_SCHEMA))
        .apply(
            IcebergIO.writeRows(
                    IcebergCatalogConfig.builder()
                        .setCatalogName("local")
                        .setCatalogProperties(catalogProps)
                        .build())
                .to(TableIdentifier.parse("demo.orders")));
    return pipeline;
  }

  /** Runs the visitor factory over the pipeline graph and collects one direction. */
  private static List<DatasetIdentifier> extract(Pipeline pipeline, boolean inputs) {
    VisitorFactory visitorFactory = new VisitorFactory();
    List<DatasetIdentifier> result = new java.util.ArrayList<>();
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(
              org.apache.beam.sdk.runners.TransformHierarchy.Node node) {
            inspect(node.getTransform());
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(
              org.apache.beam.sdk.runners.TransformHierarchy.Node node) {
            inspect(node.getTransform());
          }

          private void inspect(PTransform<?, ?> transform) {
            if (transform == null) {
              return;
            }
            result.addAll(
                inputs
                    ? visitorFactory.extractInputs(transform)
                    : visitorFactory.extractOutputs(transform));
          }
        });
    return result;
  }

  /** A user transform exposing its datasets through the public extension point. */
  private static class ProviderTransform
      extends PTransform<org.apache.beam.sdk.values.PBegin, PCollection<String>>
      implements LineageProvider {
    @Override
    public List<DatasetIdentifier> getInputDatasets() {
      return java.util.Collections.singletonList(
          new DatasetIdentifier("events", "kafka://broker:9092"));
    }

    @Override
    public PCollection<String> expand(org.apache.beam.sdk.values.PBegin input) {
      return input.apply(org.apache.beam.sdk.transforms.Create.of("a"));
    }
  }
}
