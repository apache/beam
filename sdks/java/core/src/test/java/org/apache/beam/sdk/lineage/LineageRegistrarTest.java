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
package org.apache.beam.sdk.lineage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LineageRegistrar} ServiceLoader discovery and DirectRunner integration. */
@RunWith(JUnit4.class)
public class LineageRegistrarTest {

  /**
   * TestWatcher that logs detailed lineage diagnostics only when tests fail.
   * This keeps successful test output clean while providing deep debugging for failures.
   */
  @Rule
  public TestWatcher lineageDebugLogger = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      System.err.println("=== Lineage Test Failure Diagnostics ===");
      System.err.println("Test: " + description.getMethodName());
      System.err.println("Error: " + e.getMessage());

      List<String> sources = TestLineage.getRecordedSources();
      List<String> sinks = TestLineage.getRecordedSinks();

      System.err.println("\nRecorded Sources (" + sources.size() + "):");
      for (int i = 0; i < sources.size(); i++) {
        System.err.println("  [" + i + "] \"" + sources.get(i) + "\"");
      }

      System.err.println("\nRecorded Sinks (" + sinks.size() + "):");
      for (int i = 0; i < sinks.size(); i++) {
        System.err.println("  [" + i + "] \"" + sinks.get(i) + "\"");
      }

      System.err.println("========================================");
    }
  };

  @Before
  public void setUp() {
    // Clear any recorded lineage from previous tests
    TestLineage.clearRecorded();
  }

  /** Helper to create a TestPipeline with test lineage enabled. */
  private TestPipeline createTestPipelineWithLineage() {
    TestLineageOptions options = PipelineOptionsFactory.create().as(TestLineageOptions.class);
    options.setEnableTestLineage(true);
    TestPipeline pipeline = TestPipeline.fromOptions(options);
    // Disable enforcement since we're not using @Rule
    pipeline.enableAbandonedNodeEnforcement(false);
    return pipeline;
  }

  @Test
  public void testServiceLoaderDiscovery() {
    // Load all LineageRegistrar implementations via ServiceLoader
    for (LineageRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(LineageRegistrar.class).iterator())) {

      // Check if we found the TestLineageRegistrar
      if (registrar instanceof TestLineageRegistrar) {

        // Create options with test lineage enabled
        TestLineageOptions options = PipelineOptionsFactory.create().as(TestLineageOptions.class);
        options.setEnableTestLineage(true);

        // Test with SOURCE direction
        Lineage sourceLineage = registrar.fromOptions(options, Lineage.LineageDirection.SOURCE);
        assertThat(sourceLineage, notNullValue());
        assertThat(sourceLineage, instanceOf(TestLineage.class));
        assertEquals(Lineage.LineageDirection.SOURCE, ((TestLineage) sourceLineage).getDirection());

        // Test with SINK direction
        Lineage sinkLineage = registrar.fromOptions(options, Lineage.LineageDirection.SINK);
        assertThat(sinkLineage, notNullValue());
        assertThat(sinkLineage, instanceOf(TestLineage.class));
        assertEquals(Lineage.LineageDirection.SINK, ((TestLineage) sinkLineage).getDirection());

        return;
      }
    }

    fail("Expected to find " + TestLineageRegistrar.class);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testLineageIntegrationWithSimpleFQN() {
    // Create pipeline with test lineage enabled - Lineage will be initialized during pipeline.run()
    TestPipeline pipeline = createTestPipelineWithLineage();

    // Run pipeline that records lineage
    pipeline
        .apply(Create.of("a", "b", "c"))
        .apply(ParDo.of(new RecordSourceLineageDoFn("testsystem", Arrays.asList("db", "table"))));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Verify lineage was recorded
    List<String> sources = TestLineage.getRecordedSources();
    assertThat(sources, hasItem("testsystem:db.table"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testLineageIntegrationWithSubtype() {
    // Create pipeline with test lineage enabled - Lineage will be initialized during pipeline.run()
    TestPipeline pipeline = createTestPipelineWithLineage();

    // Run pipeline that records lineage with subtype
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(
            ParDo.of(
                new RecordSourceLineageWithSubtypeDoFn(
                    "spanner",
                    "table",
                    Arrays.asList("project", "instance", "database", "table"))));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Verify lineage was recorded with subtype
    List<String> sources = TestLineage.getRecordedSources();
    assertThat(sources, hasItem("spanner:table:project.instance.database.table"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testLineageIntegrationWithLastSegmentSeparator() {
    // Create pipeline with test lineage enabled - Lineage will be initialized during pipeline.run()
    TestPipeline pipeline = createTestPipelineWithLineage();

    // Run pipeline that records lineage with custom separator
    pipeline
        .apply(Create.of("x", "y", "z"))
        .apply(
            ParDo.of(
                new RecordSourceLineageWithSeparatorDoFn(
                    "gcs", Arrays.asList("bucket", "path/to/file.txt"), "/")));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Verify lineage was recorded with separator
    List<String> sources = TestLineage.getRecordedSources();
    assertThat(sources, hasItem("gcs:bucket.path/to/file.txt"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testLineageIntegrationWithBothSourcesAndSinks() {
    // Create pipeline with test lineage enabled - Lineage will be initialized during pipeline.run()
    TestPipeline pipeline = createTestPipelineWithLineage();

    // Run pipeline that records both source and sink lineage
    pipeline
        .apply(Create.of("data1", "data2"))
        .apply(ParDo.of(new RecordBothSourceAndSinkLineageDoFn()));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Verify both source and sink lineage were recorded
    List<String> sources = TestLineage.getRecordedSources();
    List<String> sinks = TestLineage.getRecordedSinks();

    assertThat(sources, hasItem("input-system:input-db.input-table"));
    assertThat(sinks, hasItem("output-system:output-db.output-table"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testLineageIntegrationWithMultipleElements() {
    // Create pipeline with test lineage enabled - Lineage will be initialized during pipeline.run()
    TestPipeline pipeline = createTestPipelineWithLineage();

    // Run pipeline with multiple elements to test thread safety
    pipeline
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        .apply(ParDo.of(new RecordSourceLineageDoFn("system", Arrays.asList("resource"))));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Verify lineage was recorded for all elements (may have duplicates)
    List<String> sources = TestLineage.getRecordedSources();
    assertThat(sources, hasSize(10)); // One per element
    assertThat(sources, hasItem("system:resource"));
  }

  // Helper DoFn classes for recording lineage

  /** DoFn that records source lineage with simple FQN. */
  private static class RecordSourceLineageDoFn<T> extends DoFn<T, T> {
    private final String system;
    private final List<String> segments;

    RecordSourceLineageDoFn(String system, List<String> segments) {
      this.system = system;
      this.segments = segments;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Lineage.getSources().add(system, segments);
      c.output(c.element());
    }
  }

  /** DoFn that records source lineage with subtype. */
  private static class RecordSourceLineageWithSubtypeDoFn extends DoFn<Integer, Integer> {
    private final String system;
    private final String subtype;
    private final List<String> segments;

    RecordSourceLineageWithSubtypeDoFn(String system, String subtype, List<String> segments) {
      this.system = system;
      this.subtype = subtype;
      this.segments = segments;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Lineage.getSources().add(system, subtype, segments, null);
      c.output(c.element());
    }
  }

  /** DoFn that records source lineage with custom last segment separator. */
  private static class RecordSourceLineageWithSeparatorDoFn extends DoFn<String, String> {
    private final String system;
    private final List<String> segments;
    private final String separator;

    RecordSourceLineageWithSeparatorDoFn(String system, List<String> segments, String separator) {
      this.system = system;
      this.segments = segments;
      this.separator = separator;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Lineage.getSources().add(system, segments, separator);
      c.output(c.element());
    }
  }

  /** DoFn that records both source and sink lineage. */
  private static class RecordBothSourceAndSinkLineageDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Lineage.getSources().add("input-system", ImmutableList.of("input-db", "input-table"));
      Lineage.getSinks().add("output-system", ImmutableList.of("output-db", "output-table"));
      c.output(c.element());
    }
  }
}
