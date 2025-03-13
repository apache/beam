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
package org.apache.beam.sdk.extensions.sql.zetasql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Tests related to {@code BeamZetaSqlCalcRel}. */
public class BeamZetaSqlCalcRelTest extends ZetaSqlTestBase {

  private PCollection<Row> compile(String sql) {
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, QueryParameters.ofNone());
    return BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    initialize();
  }

  private static class NodeGetter extends Pipeline.PipelineVisitor.Defaults {

    private final PValue target;
    private TransformHierarchy.Node producer;

    private NodeGetter(PValue target) {
      this.target = target;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      if (value == target) {
        assert this.producer == null;
        this.producer = producer;
      }
    }
  }

  @Test
  public void testSingleFieldAccess() throws IllegalAccessException {
    String sql = "SELECT Key FROM KeyValue";

    PCollection<Row> rows = compile(sql);

    final NodeGetter nodeGetter = new NodeGetter(rows);
    pipeline.traverseTopologically(nodeGetter);

    ParDo.MultiOutput<Row, Row> pardo =
        (ParDo.MultiOutput<Row, Row>) nodeGetter.producer.getTransform();
    PCollection<Row> input =
        (PCollection<Row>) Iterables.getOnlyElement(nodeGetter.producer.getInputs().values());

    DoFnSchemaInformation info = ParDo.getDoFnSchemaInformation(pardo.getFn(), input);

    FieldAccessDescriptor fieldAccess = info.getFieldAccessDescriptor();

    Assert.assertTrue(fieldAccess.referencesSingleField());
    Assert.assertEquals("Key", Iterables.getOnlyElement(fieldAccess.fieldNamesAccessed()));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testNoFieldAccess() throws IllegalAccessException {
    String sql = "SELECT 1 FROM KeyValue";

    PCollection<Row> rows = compile(sql);

    final NodeGetter nodeGetter = new NodeGetter(rows);
    pipeline.traverseTopologically(nodeGetter);

    ParDo.MultiOutput<Row, Row> pardo =
        (ParDo.MultiOutput<Row, Row>) nodeGetter.producer.getTransform();
    PCollection<Row> input =
        (PCollection<Row>) Iterables.getOnlyElement(nodeGetter.producer.getInputs().values());

    DoFnSchemaInformation info = ParDo.getDoFnSchemaInformation(pardo.getFn(), input);

    FieldAccessDescriptor fieldAccess = info.getFieldAccessDescriptor();

    Assert.assertFalse(fieldAccess.getAllFields());
    Assert.assertTrue(fieldAccess.getFieldsAccessed().isEmpty());
    Assert.assertTrue(fieldAccess.getNestedFieldsAccessed().isEmpty());

    pipeline.run().waitUntilFinish();
  }
}
