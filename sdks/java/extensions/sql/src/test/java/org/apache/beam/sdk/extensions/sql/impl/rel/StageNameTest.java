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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for provenance-based naming of the composites a Beam SQL plan expands into. */
public class StageNameTest extends BaseRelTest {

  @BeforeClass
  public static void prepare() {
    registerTable(
        "ORDERS",
        TestBoundedTable.of(
                Schema.FieldType.INT64, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.DECIMAL, "price")
            .addRows(1L, 1, new BigDecimal(1.0), 2L, 2, new BigDecimal(2.0)));
  }

  @Test
  public void composeDeduplicatesAndRestoresPlanOrder() {
    assertThat(
        StageName.compose(Arrays.asList("7:Project", "3:Filter", "7:Project")),
        is("3:Filter;7:Project"));
  }

  @Test
  public void composeSkipsMissingLabels() {
    assertThat(StageName.compose(Arrays.asList(null, "", "3:Filter", null)), is("3:Filter"));
  }

  /**
   * A leaf is named after the table it scans, which could contain the characters the stored form
   * uses to delimit parts and so make one label parse as several.
   */
  @Test
  public void separatorsInASourceNameAreNeutralized() {
    assertThat(StageName.sanitize("a;b:c"), is("a_b_c"));
    assertThat(
        StageName.render(StageName.compose(Arrays.asList("3:" + StageName.sanitize("a;b:c")))),
        is("a_b_c"));
  }

  /**
   * The property that keeps the planner terminating: transpose rules match the same pair of nodes
   * in either order, and a label that changed with the order would be an endless supply of rels the
   * planner has not seen before.
   */
  @Test
  public void composeIsIndependentOfMatchOrder() {
    String forwards = StageName.compose(Arrays.asList("3:Filter", "7:Project"));
    String backwards = StageName.compose(Arrays.asList("7:Project", "3:Filter"));
    assertThat(backwards, is(forwards));
    assertThat(StageName.compose(Arrays.asList(forwards, backwards)), is(forwards));
  }

  @Test
  public void aLongChainIsElidedInTheMiddle() {
    List<String> labels = new ArrayList<>();
    labels.add("0:first");
    for (int i = 0; i < 40; i++) {
      labels.add((i + 1) + ":Project" + i);
    }
    labels.add("41:last");
    assertThat(StageName.render(StageName.compose(labels)), is("first;+40 more;last"));
  }

  @Test
  public void fusedCalcIsNamedAfterEverythingFusedIntoIt() {
    assertThat(
        topLevelNames("SELECT order_id + 1 FROM ORDERS WHERE site_id = 1"),
        hasItem("Filter;Project"));
  }

  @Test
  public void repeatedNamesAreDisambiguatedInPlanOrder() {
    List<String> names =
        topLevelNames(
            "SELECT order_id FROM ORDERS WHERE site_id = 1 "
                + "UNION ALL SELECT order_id FROM ORDERS WHERE site_id = 2");
    assertThat(names.stream().filter(n -> n.startsWith("Filter;Project")).count(), is(2L));
    assertThat(names, hasItem("Filter;Project #2"));
  }

  @Test
  public void legacyExperimentRestoresClassNameAndId() {
    List<String> names =
        topLevelNames(
            "SELECT order_id + 1 FROM ORDERS WHERE site_id = 1", StageName.LEGACY_EXPERIMENT);
    assertThat(names, not(empty()));
    assertThat(names.stream().allMatch(n -> n.matches("Beam\\w+Rel_\\d+")), is(true));
  }

  /**
   * The one property the whole scheme is for. Labels are only invisible to the planner's digest for
   * some rel types -- {@code Calc} among them -- so which of two equally valid labels survives can
   * come down to the order rels were registered in. If that ever varies between runs, so do the
   * names, and Dataflow streaming update breaks on a pipeline nobody edited.
   */
  @Test
  public void namesAreIdenticalAcrossRuns() {
    String sql =
        "SELECT order_id + 1, price * 2 FROM ORDERS WHERE site_id = 1 AND order_id > 0 "
            + "UNION ALL SELECT order_id, price FROM ORDERS WHERE site_id = 2";
    List<String> first = topLevelNames(sql);
    for (int run = 0; run < 4; run++) {
      assertThat(topLevelNames(sql), is(first));
    }
  }

  /**
   * {@code PROJECT_FILTER_TRANSPOSE} and {@code FILTER_PROJECT_TRANSPOSE} are both in Beam's rule
   * set, so they swap a Filter/Project pair back and forth indefinitely. A label that depended on
   * the order the pair was matched in would make each swap produce a rel the planner had not seen,
   * and planning would never converge. Several stacked pairs give the planner room to do it.
   */
  @Test(timeout = 120_000)
  public void planningTerminatesWhenTransposeRulesCycle() {
    List<String> names =
        topLevelNames(
            "SELECT a + 1 AS a, b FROM ("
                + "  SELECT a, b FROM ("
                + "    SELECT order_id AS a, price AS b FROM ORDERS WHERE site_id = 1"
                + "  ) WHERE a > 0"
                + ") WHERE b > 0");
    assertThat(names, not(empty()));
  }

  /**
   * Occurrence numbering restarts with each query. Every caller expands a plan either into a fresh
   * pipeline or inside {@code SqlTransform}'s own composite, so names only have to be unique among
   * the stages of one query -- and counting per query means a second query cannot renumber the
   * stages of the first.
   */
  @Test
  public void numberingRestartsForEachQuery() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(CrashingRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    String sql =
        "SELECT order_id FROM ORDERS WHERE site_id = 1 "
            + "UNION ALL SELECT order_id FROM ORDERS WHERE site_id = 2";
    pipeline.apply("first", new Query(sql));
    pipeline.apply("second", new Query(sql));

    List<String> first = namesUnder(pipeline, "first");
    assertThat(first, hasItem("Filter;Project #2"));
    assertThat(namesUnder(pipeline, "second"), is(first));
  }

  /** Applies a query the way {@code SqlTransform} does, inside a composite of its own. */
  private static class Query extends PTransform<PBegin, PCollection<Row>> {
    private final String sql;

    Query(String sql) {
      this.sql = sql;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      return BeamSqlRelUtils.toPCollection(input.getPipeline(), env.parseQuery(sql));
    }
  }

  private static List<String> namesUnder(Pipeline pipeline, String prefix) {
    List<String> names = new ArrayList<>();
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            String full = node.getFullName();
            if (full.startsWith(prefix + "/")) {
              String rest = full.substring(prefix.length() + 1);
              if (!rest.contains("/")) {
                names.add(rest);
              }
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    return names;
  }

  private static List<String> topLevelNames(String sql, String... experiments) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(CrashingRunner.class);
    for (String experiment : experiments) {
      ExperimentalOptions.addExperiment(options.as(ExperimentalOptions.class), experiment);
    }
    Pipeline pipeline = Pipeline.create(options);
    compilePipeline(sql, pipeline);
    return topLevelNames(pipeline);
  }

  private static List<String> topLevelNames(Pipeline pipeline) {
    List<String> names = new ArrayList<>();
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (!node.isRootNode() && !node.getFullName().contains("/")) {
              names.add(node.getFullName());
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    return names;
  }
}
