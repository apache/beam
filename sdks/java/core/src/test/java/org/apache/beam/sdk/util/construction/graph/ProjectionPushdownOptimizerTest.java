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

import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
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
    SimpleSource source =
        new SimpleSource(FieldAccessDescriptor.withFieldNames("foo", "bar", "baz"));
    p.apply(source)
        .apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("foo", "bar")));

    ProjectionPushdownOptimizer.optimize(p);
    Assert.assertTrue(pipelineHasTransform(p, source));
  }

  @Test
  public void testSimpleProjectionPushdown() {
    Pipeline p = Pipeline.create();
    SimpleSourceWithPushdown originalSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withFieldNames("foo", "bar", "baz"));
    FieldAccessDescriptor downstreamFieldAccess =
        FieldAccessDescriptor.withFieldNames("foo", "bar");
    p.apply(originalSource).apply(new FieldAccessTransform(downstreamFieldAccess));

    SimpleSourceWithPushdown expectedSource = new SimpleSourceWithPushdown(downstreamFieldAccess);

    ProjectionPushdownOptimizer.optimize(p);
    Assert.assertTrue(pipelineHasTransform(p, expectedSource));
    Assert.assertFalse(pipelineHasTransform(p, originalSource));
  }

  @Test
  public void testBranchedProjectionPushdown() {
    Pipeline p = Pipeline.create();
    SimpleSourceWithPushdown originalSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withFieldNames("foo", "bar", "baz"));
    PCollection<Row> input = p.apply(originalSource);
    input.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("foo")));
    input.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("bar")));

    SimpleSourceWithPushdown expectedSource =
        new SimpleSourceWithPushdown(FieldAccessDescriptor.withFieldNames("foo", "bar"));

    ProjectionPushdownOptimizer.optimize(p);
    Assert.assertTrue(pipelineHasTransform(p, expectedSource));
    Assert.assertFalse(pipelineHasTransform(p, originalSource));
  }

  @Test
  public void testIntermediateProducer() {
    Pipeline p = Pipeline.create();
    SimpleSource source =
        new SimpleSource(FieldAccessDescriptor.withFieldNames("foo", "bar", "baz"));
    IntermediateTransformWithPushdown originalT =
        new IntermediateTransformWithPushdown(
            FieldAccessDescriptor.withFieldNames("foo", "bar", "baz"));
    FieldAccessDescriptor downstreamFieldAccess =
        FieldAccessDescriptor.withFieldNames("foo", "bar");
    p.apply(source).apply(originalT).apply(new FieldAccessTransform(downstreamFieldAccess));

    // TODO(https://github.com/apache/beam/issues/21359) Support pushdown on intermediate
    // transforms. For now, test that the pushdown optimizer ignores immediate transforms.
    ProjectionPushdownOptimizer.optimize(p);
    Assert.assertTrue(pipelineHasTransform(p, originalT));
  }

  @Test
  public void testMultipleOutputs() {
    Pipeline p = Pipeline.create();
    MultipleOutputSourceWithPushdown originalSource =
        new MultipleOutputSourceWithPushdown(
            FieldAccessDescriptor.withFieldNames("foo", "bar", "baz"),
            FieldAccessDescriptor.withFieldNames("qux", "quux", "quuz"));
    PCollectionTuple inputs = p.apply(originalSource);
    inputs
        .get(originalSource.tag1)
        .apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("foo")));
    inputs
        .get(originalSource.tag1)
        .apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("bar")));
    inputs
        .get(originalSource.tag2)
        .apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("qux")));
    inputs
        .get(originalSource.tag2)
        .apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("quux")));

    MultipleOutputSourceWithPushdown expectedSource =
        new MultipleOutputSourceWithPushdown(
            FieldAccessDescriptor.withFieldNames("foo", "bar"),
            FieldAccessDescriptor.withFieldNames("qux", "quux"));

    ProjectionPushdownOptimizer.optimize(p);
    Assert.assertTrue(pipelineHasTransform(p, expectedSource));
    Assert.assertFalse(pipelineHasTransform(p, originalSource));
  }

  private static boolean pipelineHasTransform(Pipeline p, PTransform<?, ?> t) {
    HasTransformVisitor hasTransformVisitor = new HasTransformVisitor(t);
    p.traverseTopologically(hasTransformVisitor);
    return hasTransformVisitor.found;
  }

  private static class HasTransformVisitor extends Defaults {
    private final PTransform<?, ?> t;
    boolean found = false;

    HasTransformVisitor(PTransform<?, ?> t) {
      this.t = t;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {
      if (t.equals(node.getTransform())) {
        found = true;
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }
  }

  private static class FieldAccessTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
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

  private static Schema createStringSchema(FieldAccessDescriptor fieldAccessDescriptor) {
    Schema.Builder schemaBuilder = Schema.builder();
    for (FieldDescriptor field : fieldAccessDescriptor.getFieldsAccessed()) {
      schemaBuilder.addStringField(field.getFieldName());
    }
    return schemaBuilder.build();
  }

  private abstract static class SchemaSourceTransform<InputT extends PInput>
      extends PTransform<InputT, PCollection<Row>> {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    protected final Schema schema;

    SchemaSourceTransform(FieldAccessDescriptor fieldAccessDescriptor) {
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
      SchemaSourceTransform<?> that = (SchemaSourceTransform<?>) o;
      return Objects.equals(
          fieldAccessDescriptor.fieldNamesAccessed(),
          that.fieldAccessDescriptor.fieldNamesAccessed());
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldAccessDescriptor.fieldNamesAccessed());
    }
  }

  private static class SimpleSource extends SchemaSourceTransform<PBegin> {

    SimpleSource(FieldAccessDescriptor fieldAccessDescriptor) {
      super(fieldAccessDescriptor);
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input.apply(Impulse.create()).apply(ParDo.of(new NoOpDoFn<>())).setRowSchema(schema);
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

  private static class IntermediateTransformWithPushdown
      extends SchemaSourceTransform<PCollection<Row>>
      implements ProjectionProducer<PTransform<PCollection<Row>, PCollection<Row>>> {

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
      Assert.assertEquals(
          new TupleTag<>("output"), Iterables.getOnlyElement(outputFields.keySet()));
      return new IntermediateTransformWithPushdown(Iterables.getOnlyElement(outputFields.values()));
    }
  }

  private static class NoOpDoFn<T> extends DoFn<T, Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {}
  }

  private static class MultipleOutputSourceWithPushdown extends PTransform<PBegin, PCollectionTuple>
      implements ProjectionProducer<PTransform<PBegin, PCollectionTuple>> {
    private final FieldAccessDescriptor fieldAccessDescriptor1;
    private final FieldAccessDescriptor fieldAccessDescriptor2;
    protected final Schema schema1;
    protected final Schema schema2;

    final TupleTag<Row> tag1 = new TupleTag<>("output1");
    final TupleTag<Row> tag2 = new TupleTag<>("output2");

    MultipleOutputSourceWithPushdown(
        FieldAccessDescriptor fieldAccessDescriptor1,
        FieldAccessDescriptor fieldAccessDescriptor2) {
      this.fieldAccessDescriptor1 = fieldAccessDescriptor1;
      this.fieldAccessDescriptor2 = fieldAccessDescriptor2;
      this.schema1 = createStringSchema(fieldAccessDescriptor1);
      this.schema2 = createStringSchema(fieldAccessDescriptor2);
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
      PCollectionTuple outputs =
          input
              .apply(Impulse.create())
              .apply(
                  ParDo.of(new NoOpDoFn<>())
                      .withOutputTags(
                          new TupleTag<>("output1"), TupleTagList.of(new TupleTag<>("output2"))));
      outputs.get(tag1).setRowSchema(schema1);
      outputs.get(tag2).setRowSchema(schema2);
      return outputs;
    }

    @Override
    public boolean supportsProjectionPushdown() {
      return true;
    }

    @Override
    public PTransform<PBegin, PCollectionTuple> actuateProjectionPushdown(
        Map<TupleTag<?>, FieldAccessDescriptor> fields) {
      return new MultipleOutputSourceWithPushdown(fields.get(tag1), fields.get(tag2));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MultipleOutputSourceWithPushdown that = (MultipleOutputSourceWithPushdown) o;
      return Objects.equals(
              fieldAccessDescriptor1.fieldNamesAccessed(),
              that.fieldAccessDescriptor1.fieldNamesAccessed())
          && Objects.equals(
              fieldAccessDescriptor2.fieldNamesAccessed(),
              that.fieldAccessDescriptor2.fieldNamesAccessed());
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          fieldAccessDescriptor1.fieldNamesAccessed(), fieldAccessDescriptor2.fieldNamesAccessed());
    }
  }
}
