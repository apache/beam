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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

/** Tests for {@link FieldAccessVisitor}. */
public class FieldAccessVisitorTest {
  @Test
  public void testFieldAccessKnownMainInput() {
    Pipeline p = Pipeline.create();
    FieldAccessVisitor fieldAccessVisitor = new FieldAccessVisitor();
    Schema schema =
        Schema.of(Field.of("field1", FieldType.STRING), Field.of("field2", FieldType.STRING));
    PCollection<Row> source =
        p.apply(Create.of(Row.withSchema(schema).addValues("foo", "bar").build()))
            .setRowSchema(schema);
    source.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("field1")));

    p.traverseTopologically(fieldAccessVisitor);

    FieldAccessDescriptor fieldAccess = fieldAccessVisitor.getPCollectionFieldAccess().get(source);
    assertFalse(fieldAccess.getAllFields());
    assertThat(fieldAccess.fieldNamesAccessed(), containsInAnyOrder("field1"));
  }

  @Test
  public void testFieldAccessTwoKnownMainInputs() {
    Pipeline p = Pipeline.create();
    FieldAccessVisitor fieldAccessVisitor = new FieldAccessVisitor();
    Schema schema =
        Schema.of(
            Field.of("field1", FieldType.STRING),
            Field.of("field2", FieldType.STRING),
            Field.of("field3", FieldType.STRING));
    PCollection<Row> source =
        p.apply(Create.of(Row.withSchema(schema).addValues("foo", "bar", "baz").build()))
            .setRowSchema(schema);
    source.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("field1")));
    source.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("field2")));

    p.traverseTopologically(fieldAccessVisitor);

    FieldAccessDescriptor fieldAccess = fieldAccessVisitor.getPCollectionFieldAccess().get(source);
    assertFalse(fieldAccess.getAllFields());
    assertThat(fieldAccess.fieldNamesAccessed(), containsInAnyOrder("field1", "field2"));
  }

  @Test
  public void testFieldAccessNoSchema() {
    Pipeline p = Pipeline.create();
    FieldAccessVisitor fieldAccessVisitor = new FieldAccessVisitor();
    PCollection<String> source = p.apply(Create.of("foo", "bar"));
    source.apply(ParDo.of(new StringDoFn()));

    p.traverseTopologically(fieldAccessVisitor);

    // getAllFields is vacuously true (since there are no fields to access).
    assertTrue(fieldAccessVisitor.getPCollectionFieldAccess().get(source).getAllFields());
  }

  @Test
  public void testFieldAccessUnknownMainInput() {
    Pipeline p = Pipeline.create();
    FieldAccessVisitor fieldAccessVisitor = new FieldAccessVisitor();
    Schema schema =
        Schema.of(Field.of("field1", FieldType.STRING), Field.of("field2", FieldType.STRING));
    PCollection<Row> source =
        p.apply(Create.of(Row.withSchema(schema).addValues("foo", "bar").build()))
            .setRowSchema(schema);
    source.apply(ParDo.of(new UnknownDoFn())).setRowSchema(schema);

    p.traverseTopologically(fieldAccessVisitor);

    assertTrue(fieldAccessVisitor.getPCollectionFieldAccess().get(source).getAllFields());
  }

  @Test
  public void testFieldAccessKnownAndUnknownMainInputs() {
    Pipeline p = Pipeline.create();
    FieldAccessVisitor fieldAccessVisitor = new FieldAccessVisitor();
    Schema schema =
        Schema.of(Field.of("field1", FieldType.STRING), Field.of("field2", FieldType.STRING));
    PCollection<Row> source =
        p.apply(Create.of(Row.withSchema(schema).addValues("foo", "bar").build()))
            .setRowSchema(schema);
    source.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("field1")));
    source.apply(ParDo.of(new UnknownDoFn())).setRowSchema(schema);

    p.traverseTopologically(fieldAccessVisitor);

    assertTrue(fieldAccessVisitor.getPCollectionFieldAccess().get(source).getAllFields());
  }

  @Test
  public void testFieldAccessKnownMainAndUnknownSideInputs() {
    Pipeline p = Pipeline.create();
    FieldAccessVisitor fieldAccessVisitor = new FieldAccessVisitor();
    Schema schema =
        Schema.of(Field.of("field1", FieldType.STRING), Field.of("field2", FieldType.STRING));

    PCollection<Row> source1 =
        p.apply(Create.of(Row.withSchema(schema).addValues("foo", "bar").build()))
            .setRowSchema(schema);
    source1.apply(new FieldAccessTransform(FieldAccessDescriptor.withFieldNames("field1")));
    PCollectionView<Row> source1View = source1.apply(View.asSingleton());
    PCollection<Row> source2 =
        p.apply(Create.of(Row.withSchema(schema).addValues("baz", "qux").build()))
            .setRowSchema(schema);
    source2
        .apply(ParDo.of(new UnknownDoFn()).withSideInput("source1View", source1View))
        .setRowSchema(schema);

    p.traverseTopologically(fieldAccessVisitor);

    assertTrue(fieldAccessVisitor.getPCollectionFieldAccess().get(source1).getAllFields());
    assertTrue(fieldAccessVisitor.getPCollectionFieldAccess().get(source2).getAllFields());
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
          .setRowSchema(getSchema(fieldAccessDescriptor));
    }
  }

  /** Just some random DoFn without field access information. */
  private static class UnknownDoFn extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      // Do nothing; we don't need to execute this DoFn.
    }
  }

  /** Just some random DoFn that process raw strings, no Rows or Schemas involved. */
  private static class StringDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      // Do nothing; we don't need to execute this DoFn.
    }
  }

  private static Schema getSchema(FieldAccessDescriptor fieldAccessDescriptor) {
    Schema.Builder schemaBuilder = Schema.builder();
    for (FieldDescriptor field : fieldAccessDescriptor.getFieldsAccessed()) {
      schemaBuilder.addStringField(field.getFieldName());
    }
    return schemaBuilder.build();
  }
}
