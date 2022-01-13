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
package org.apache.beam.runners.core.construction.graph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jdk.internal.util.xml.impl.Input;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.Defaults;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ProjectionPushdownOptimizer}. */
@RunWith(JUnit4.class)
public class ProjectionPushdownOptimizerTest {

  @Test
  public void testSourceDoesNotImplementPushdownProjector() {
    Pipeline p = Pipeline.create();
    p.apply(new SimpleSource(FieldAccessDescriptor.withAllFields()))
        .apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("foo", "bar")));

    ProjectionPushdownOptimizer.optimize(p);
    // TODO(ibzib) check
  }

  @Test
  public void testSimpleProjectionPushdown() {
    Pipeline p = Pipeline.create();
    SimpleSourceWithPushdown originalSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withAllFields());
    FieldAccessDescriptor downstreamFieldAccess =
        FieldAccessDescriptor.withFieldNames("foo", "bar");
    p.apply(originalSource).apply(new FieldAccessTransform(downstreamFieldAccess));

    SimpleSourceWithPushdown expectedSource = new SimpleSourceWithPushdown(downstreamFieldAccess);
    testProjectionPushdown(
        p,
        ImmutableList.of(expectedSource),
        ImmutableList.of(originalSource) /* unexpectedSources */);
  }

  @Test
  public void testBranchedProjectionPushdown() {
    Pipeline p = Pipeline.create();
    SimpleSourceWithPushdown originalSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withAllFields());
    PCollection<Row> input = p.apply(originalSource);
    input.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("foo")));
    input.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("bar")));

    SimpleSourceWithPushdown expectedSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withFieldNames("foo", "bar"));
    testProjectionPushdown(p, ImmutableList.of(expectedSource), ImmutableList.of(originalSource));
  }

  // TODO(ibzib) test pardo with side inputs
  @Test
  public void testBranchedProjectionPushdow2n() {
    Pipeline p = Pipeline.create();
    SimpleSourceWithPushdown originalSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withAllFields());
    PCollection<Row> input = p.apply(originalSource);
    input.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("foo")));
    input.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("bar")));

    SimpleSourceWithPushdown expectedSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withFieldNames("foo", "bar"));
    testProjectionPushdown(p, ImmutableList.of(expectedSource), ImmutableList.of(originalSource));
  }

  @Test
  public void testIntermediateProducer() {
    Pipeline p = Pipeline.create();
    SimpleSource source =
        new SimpleSource(FieldAccessDescriptor.withAllFields());
    IntermediateTransformWithPushdown originalT = new IntermediateTransformWithPushdown(FieldAccessDescriptor.withAllFields());
    FieldAccessDescriptor downstreamFieldAccess =
        FieldAccessDescriptor.withFieldNames("foo", "bar");
    p.apply(source).apply(originalT).apply(new FieldAccessTransform(downstreamFieldAccess));

    IntermediateTransformWithPushdown expectedT =
        new IntermediateTransformWithPushdown(FieldAccessDescriptor.withFieldNames("foo", "bar"));
    testProjectionPushdown(p, ImmutableList.of(source, expectedT), ImmutableList.of(originalT));
  }

  // TODO(ibzib) test unknown ptransform

  private void testProjectionPushdown(
      Pipeline p, List<SchemaTransform<?>> expectedSources, List<SchemaTransform<?>> unexpectedSources) {
    ProjectionPushdownOptimizer.optimize(p);

    PipelineVisitor pipelineVisitor =
        new Defaults() {
          final List<PTransform<?, ?>> foundSources = new ArrayList<>();

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            for (SchemaTransform<?> expected : expectedSources) {
              if (expected.equals(node.getTransform())) {
                foundSources.add(node.getTransform());
              }
            }
            for (SchemaTransform<?> unexpected : unexpectedSources) {
              Assert.assertNotEquals(unexpected, node.getTransform());
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void leavePipeline(Pipeline p) {
            Assert.assertEquals(
                "Not all expected sources found in optimized pipeline.",
                expectedSources,
                foundSources);
            super.leavePipeline(p);
          }
        };

    p.traverseTopologically(pipelineVisitor);
  }

  static class FieldAccessTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
    private final FieldAccessDescriptor fieldAccessDescriptor;

    FieldAccessTransform(FieldAccessDescriptor fieldAccessDescriptor) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
      return input
          .apply(
              ParDo.of(
                  new DoFn<Row, Row>() {
                    @SuppressWarnings("unused") // used by reflection
                    @FieldAccess("row")
                    private final FieldAccessDescriptor fieldAccessDescriptor =
                        FieldAccessTransform.this.fieldAccessDescriptor;

                    @ProcessElement
                    public void processElement(
                        @FieldAccess("row") Row row, OutputReceiver<Row> outputReceiver)
                        throws Exception {
                      // Do nothing; we don't need to execute this DoFn.
                    }
                  }))
          .setRowSchema(createStringSchema(fieldAccessDescriptor));
    }
  }

  static Schema createStringSchema(FieldAccessDescriptor fieldAccessDescriptor) {
    if (fieldAccessDescriptor.getAllFields()) {
      return createStringSchema(FieldAccessDescriptor.withFieldNames("foo", "bar", "baz"));
    }
    Schema.Builder schemaBuilder = Schema.builder();
    for (FieldDescriptor field : fieldAccessDescriptor.getFieldsAccessed()) {
      schemaBuilder.addStringField(field.getFieldName());
    }
    return schemaBuilder.build();
  }

  // TODO(ibzib) make this generic
  private abstract static class SchemaTransform<InputT extends PInput> extends PTransform<InputT, PCollection<Row>> {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    protected final Schema schema;

    SchemaTransform(FieldAccessDescriptor fieldAccessDescriptor) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.schema = createStringSchema(fieldAccessDescriptor);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SchemaTransform that = (SchemaTransform) o;
      return Objects.equals(
          fieldAccessDescriptor.fieldNamesAccessed(),
          that.fieldAccessDescriptor.fieldNamesAccessed());
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldAccessDescriptor.fieldNamesAccessed());
    }
  }

  private static class SimpleSource extends SchemaTransform<PBegin> {

    SimpleSource(FieldAccessDescriptor fieldAccessDescriptor) {
      super(fieldAccessDescriptor);
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input.apply(Impulse.create()).apply(ParDo.of(new NoOpDoFn<>())).setRowSchema(schema);
    }
  }

  private static class IntermediateTransformWithPushdown extends SchemaTransform<PCollection<Row>> implements ProjectionProducer<PTransform<PCollection<Row>, PCollection<Row>>> {

    IntermediateTransformWithPushdown(FieldAccessDescriptor fieldAccessDescriptor) {
      super(fieldAccessDescriptor);
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
      return input.apply(ParDo.of(new NoOpDoFn<>())).setRowSchema(schema);
    }

    @Override
    public boolean supportsProjectionPushdown() {
      return true;
    }

    @Override
    public PTransform<PCollection<Row>, PCollection<Row>> actuateProjectionPushdown(
        Map<TupleTag<?>, FieldAccessDescriptor> outputFields) {
      return new IntermediateTransformWithPushdown(Iterables.getOnlyElement(outputFields.values()));
    }
  }

  private static class SimpleSourceWithPushdown extends SimpleSource
      implements ProjectionProducer<PTransform<PBegin, PCollection<Row>>> {

    SimpleSourceWithPushdown(FieldAccessDescriptor fieldAccessDescriptor) {
      super(fieldAccessDescriptor);
    }

    @Override
    public boolean supportsProjectionPushdown() {
      return true;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> actuateProjectionPushdown(
        Map<TupleTag<?>, FieldAccessDescriptor> fields) {
      Assert.assertEquals(new TupleTag<>("output"), Iterables.getOnlyElement(fields.keySet()));
      return new SimpleSourceWithPushdown(Iterables.getOnlyElement(fields.values()));
    }
  }

  private static class NoOpDoFn<T> extends DoFn<T, Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {}
  }

  // TODO(ibzib) test this
  @SuppressWarnings("unused") // no
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
