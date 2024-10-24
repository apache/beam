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
package org.apache.beam.sdk.util.construction.graph;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProjectionProducerVisitor}. */
@RunWith(JUnit4.class)
public class ProjectionProducerVisitorTest {
  @Test
  public void testMissingFieldAccessInformation_returnsNoPushdown() {
    Pipeline p = Pipeline.create();
    p.apply(new SimpleSource());

    Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess = ImmutableMap.of();
    ProjectionProducerVisitor visitor = new ProjectionProducerVisitor(pCollectionFieldAccess);
    p.traverseTopologically(visitor);

    Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        pushdownOpportunities = visitor.getPushdownOpportunities();
    Assert.assertTrue(pushdownOpportunities.isEmpty());
  }

  @Test
  public void testFieldAccessAllFields_returnsNoPushdown() {
    Pipeline p = Pipeline.create();
    PCollection<Row> output = p.apply(new SimpleSource());

    Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess =
        ImmutableMap.of(output, FieldAccessDescriptor.withAllFields());
    ProjectionProducerVisitor visitor = new ProjectionProducerVisitor(pCollectionFieldAccess);
    p.traverseTopologically(visitor);

    Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        pushdownOpportunities = visitor.getPushdownOpportunities();
    Assert.assertTrue(pushdownOpportunities.isEmpty());
  }

  @Test
  public void testSimplePushdownProducer_returnsOnePushdown() {
    Pipeline p = Pipeline.create();
    PTransform<PBegin, PCollection<Row>> source = new SimpleSourceWithPushdown();
    PCollection<Row> output = p.apply(source);

    Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess =
        ImmutableMap.of(output, FieldAccessDescriptor.withFieldNames("field1", "field2"));
    ProjectionProducerVisitor visitor = new ProjectionProducerVisitor(pCollectionFieldAccess);
    p.traverseTopologically(visitor);

    Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        pushdownOpportunities = visitor.getPushdownOpportunities();
    Assert.assertEquals(1, pushdownOpportunities.size());
    Map<PCollection<?>, FieldAccessDescriptor> opportunitiesForSource =
        pushdownOpportunities.get(source);
    Assert.assertNotNull(opportunitiesForSource);
    Assert.assertEquals(1, opportunitiesForSource.size());
    FieldAccessDescriptor fieldAccessDescriptor = opportunitiesForSource.get(output);
    Assert.assertNotNull(fieldAccessDescriptor);
    Assert.assertFalse(fieldAccessDescriptor.getAllFields());
    assertThat(fieldAccessDescriptor.fieldNamesAccessed(), containsInAnyOrder("field1", "field2"));
  }

  @Test
  public void testNestedPushdownProducers_returnsOnlyOutermostPushdown() {
    Pipeline p = Pipeline.create();
    PTransform<PBegin, PCollection<Row>> source = new CompositeTransformWithPushdownOutside();
    PCollection<Row> output = p.apply(source);

    Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess =
        ImmutableMap.of(output, FieldAccessDescriptor.withFieldNames("field1", "field2"));
    ProjectionProducerVisitor visitor = new ProjectionProducerVisitor(pCollectionFieldAccess);
    p.traverseTopologically(visitor);

    Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        pushdownOpportunities = visitor.getPushdownOpportunities();
    Assert.assertEquals(1, pushdownOpportunities.size());
    Map<PCollection<?>, FieldAccessDescriptor> opportunitiesForSource =
        pushdownOpportunities.get(source);
    Assert.assertNotNull(opportunitiesForSource);
    Assert.assertEquals(1, opportunitiesForSource.size());
    FieldAccessDescriptor fieldAccessDescriptor = opportunitiesForSource.get(output);
    Assert.assertNotNull(fieldAccessDescriptor);
    Assert.assertFalse(fieldAccessDescriptor.getAllFields());
    assertThat(fieldAccessDescriptor.fieldNamesAccessed(), containsInAnyOrder("field1", "field2"));
  }

  @Test
  public void testProjectionProducerInsideNonProducer_returnsInnerPushdown() {
    Pipeline p = Pipeline.create();
    CompositeTransformWithPushdownInside source = new CompositeTransformWithPushdownInside();
    PCollection<Row> output = p.apply(source);

    Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess =
        ImmutableMap.of(output, FieldAccessDescriptor.withFieldNames("field1", "field2"));
    ProjectionProducerVisitor visitor = new ProjectionProducerVisitor(pCollectionFieldAccess);
    p.traverseTopologically(visitor);

    Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        pushdownOpportunities = visitor.getPushdownOpportunities();
    Assert.assertEquals(1, pushdownOpportunities.size());
    Map<PCollection<?>, FieldAccessDescriptor> opportunitiesForSource =
        pushdownOpportunities.get(source.innerT);
    Assert.assertNotNull(opportunitiesForSource);
    Assert.assertEquals(1, opportunitiesForSource.size());
    FieldAccessDescriptor fieldAccessDescriptor = opportunitiesForSource.get(output);
    Assert.assertNotNull(fieldAccessDescriptor);
    Assert.assertFalse(fieldAccessDescriptor.getAllFields());
    assertThat(fieldAccessDescriptor.fieldNamesAccessed(), containsInAnyOrder("field1", "field2"));
  }

  @Test
  public void testPushdownProducersWithMultipleOutputs_returnsMultiplePushdowns() {
    Pipeline p = Pipeline.create();
    PTransform<PBegin, PCollectionTuple> source = new MultipleOutputSourceWithPushdown();
    PCollectionTuple outputs = p.apply(source);

    Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess =
        ImmutableMap.of(
            outputs.get("output1"),
            FieldAccessDescriptor.withFieldNames("field1", "field2"),
            outputs.get("output2"),
            FieldAccessDescriptor.withFieldNames("field3", "field4"));

    ProjectionProducerVisitor visitor = new ProjectionProducerVisitor(pCollectionFieldAccess);
    p.traverseTopologically(visitor);

    Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        pushdownOpportunities = visitor.getPushdownOpportunities();
    Assert.assertEquals(1, pushdownOpportunities.size());
    Map<PCollection<?>, FieldAccessDescriptor> opportunitiesForSource =
        pushdownOpportunities.get(source);
    Assert.assertNotNull(opportunitiesForSource);
    Assert.assertEquals(2, opportunitiesForSource.size());

    FieldAccessDescriptor fieldAccessDescriptor1 =
        opportunitiesForSource.get(outputs.get("output1"));
    Assert.assertNotNull(fieldAccessDescriptor1);
    Assert.assertFalse(fieldAccessDescriptor1.getAllFields());
    assertThat(fieldAccessDescriptor1.fieldNamesAccessed(), containsInAnyOrder("field1", "field2"));

    FieldAccessDescriptor fieldAccessDescriptor2 =
        opportunitiesForSource.get(outputs.get("output2"));
    Assert.assertNotNull(fieldAccessDescriptor2);
    Assert.assertFalse(fieldAccessDescriptor2.getAllFields());
    assertThat(fieldAccessDescriptor2.fieldNamesAccessed(), containsInAnyOrder("field3", "field4"));
  }

  private static class SimpleSource extends PTransform<PBegin, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PBegin input) {
      return input
          .apply(Impulse.create())
          .apply(ParDo.of(new NoOpDoFn<>()))
          .setRowSchema(Schema.builder().build());
    }
  }

  private static class SimpleSourceWithPushdown extends SimpleSource
      implements ProjectionProducer<PTransform<PBegin, PCollection<Row>>> {

    @Override
    public boolean supportsProjectionPushdown() {
      return true;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> actuateProjectionPushdown(
        Map<TupleTag<?>, FieldAccessDescriptor> fields) {
      return this;
    }
  }

  private static class CompositeTransformWithPushdownOutside
      extends PTransform<PBegin, PCollection<Row>>
      implements ProjectionProducer<PTransform<PBegin, PCollection<Row>>> {

    @Override
    public boolean supportsProjectionPushdown() {
      return true;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> actuateProjectionPushdown(
        Map<TupleTag<?>, FieldAccessDescriptor> fields) {
      return this;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input.apply(new SimpleSourceWithPushdown());
    }
  }

  private static class CompositeTransformWithPushdownInside
      extends PTransform<PBegin, PCollection<Row>> {
    final PTransform<PBegin, PCollection<Row>> innerT = new SimpleSourceWithPushdown();

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input.apply(innerT);
    }
  }

  private static class NoOpDoFn<T> extends DoFn<T, Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {}
  }

  private static class MultipleOutputSourceWithPushdown extends PTransform<PBegin, PCollectionTuple>
      implements ProjectionProducer<PTransform<PBegin, PCollectionTuple>> {

    @Override
    public PCollectionTuple expand(PBegin input) {
      PCollectionTuple outputs =
          input
              .apply(Impulse.create())
              .apply(
                  ParDo.of(new NoOpDoFn<>())
                      .withOutputTags(
                          new TupleTag<>("output1"), TupleTagList.of(new TupleTag<>("output2"))));
      outputs.get("output1").setRowSchema(Schema.builder().build());
      outputs.get("output2").setRowSchema(Schema.builder().build());
      return outputs;
    }

    @Override
    public boolean supportsProjectionPushdown() {
      return true;
    }

    @Override
    public PTransform<PBegin, PCollectionTuple> actuateProjectionPushdown(
        Map<TupleTag<?>, FieldAccessDescriptor> fields) {
      return this;
    }
  }
}
