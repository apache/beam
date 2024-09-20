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
package org.apache.beam.runners.dataflow.worker;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.FixedClock;
import com.google.api.client.util.Clock;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.TestOperationContext.TestDataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterDistribution;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementExecutionTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link DataflowExecutionStateTrackerTest}. */
public class DataflowExecutionStateTrackerTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private File logFolder;
  private PipelineOptions options;
  private MillisProvider clock;
  private ExecutionStateSampler sampler;
  private CounterSet counterSet;

  @Before
  public void setUp() throws IOException {
    options = PipelineOptionsFactory.create();
    clock = mock(MillisProvider.class);
    sampler = ExecutionStateSampler.newForTest(clock);
    counterSet = new CounterSet();
    logFolder = tempFolder.newFolder();
    System.setProperty(
        DataflowWorkerLoggingInitializer.RUNNER_FILEPATH_PROPERTY,
        new File(logFolder, "dataflow-json.log").getAbsolutePath());
    // We need to reset *first* because some other test may have already initialized the
    // logging initializer.
    DataflowWorkerLoggingInitializer.reset();
    DataflowWorkerLoggingInitializer.initialize();
  }

  @After
  public void tearDown() {
    DataflowWorkerLoggingInitializer.reset();
  }

  private final NameContext step1 =
      NameContext.create("stage", "originalStep1", "systemStep1", "userStep1");
  private final TestDataflowExecutionState step1Process =
      new TestDataflowExecutionState(step1, ExecutionStateTracker.PROCESS_STATE_NAME);

  @Test
  public void testReportsElementExecutionTime() throws IOException {
    enableTimePerElementExperiment();
    DataflowExecutionStateTracker tracker = createTracker();

    try (Closeable c1 = tracker.activate(new Thread())) {
      try (Closeable c2 = tracker.enterState(step1Process)) {}
      sampler.doSampling(30);
      // Execution time split evenly between executions: IDLE, step1, IDLE
    }
    assertElementProcessingTimeCounter(step1, 10, 4);
  }

  /**
   * {@link DataflowExecutionStateTrackerTest} should take one last sample when a tracker is
   * deactivated.
   */
  @Test
  public void testTakesSampleOnDeactivate() throws IOException {
    enableTimePerElementExperiment();
    DataflowExecutionStateTracker tracker = createTracker();

    try (Closeable c1 = tracker.activate(new Thread())) {
      try (Closeable c2 = tracker.enterState(step1Process)) {
        sampler.doSampling(100);
        assertThat(step1Process.getTotalMillis(), equalTo(100L));
      }
    }

    sampler.doSampling(100);
    assertThat(step1Process.getTotalMillis(), equalTo(100L));
  }

  private void enableTimePerElementExperiment() {
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(
            Lists.newArrayList(DataflowElementExecutionTracker.TIME_PER_ELEMENT_EXPERIMENT));
  }

  private void assertElementProcessingTimeCounter(NameContext step, int millis, int bucketOffset) {
    CounterName counterName = ElementExecutionTracker.COUNTER_NAME.withOriginalName(step);
    Counter<?, CounterDistribution> counter =
        (Counter<?, CounterFactory.CounterDistribution>) counterSet.getExistingCounter(counterName);
    assertNotNull(counter);

    CounterFactory.CounterDistribution distribution = counter.getAggregate();
    assertThat(
        distribution,
        equalTo(
            CounterFactory.CounterDistribution.builder()
                .minMax(millis, millis)
                .count(1)
                .sum(millis)
                .sumOfSquares(millis * millis)
                .buckets(bucketOffset, Lists.newArrayList(1L))
                .build()));
  }

  private DataflowExecutionStateTracker createTracker() {
    return new DataflowExecutionStateTracker(
        sampler,
        new TestDataflowExecutionState(NameContext.forStage("test-stage"), "other"),
        counterSet,
        options,
        "test-work-item-id");
  }

  @Test
  public void testLullReportsRightTrace() throws Exception {
    Thread mockThread = mock(Thread.class);
    StackTraceElement[] doFnStackTrace =
        new StackTraceElement[] {
          new StackTraceElement(
              "userpackage.SomeUserDoFn", "helperMethod", "SomeUserDoFn.java", 250),
          new StackTraceElement("userpackage.SomeUserDoFn", "process", "SomeUserDoFn.java", 450),
          new StackTraceElement(
              SimpleDoFnRunner.class.getName(), "processElement", "SimpleDoFnRunner.java", 500),
        };
    when(mockThread.getStackTrace()).thenReturn(doFnStackTrace);
    FixedClock clock = new FixedClock(Clock.SYSTEM.currentTimeMillis());
    DataflowExecutionStateTracker tracker = createTracker(clock);
    tracker.reportBundleLull(mockThread, 30 * 60 * 1000);
    verifyLullLog();

    clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(5L).getMillis());
    tracker.reportBundleLull(mockThread, 30 * 60 * 1000);
    verifyLullLog();

    clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(16L).getMillis());
    tracker.reportBundleLull(mockThread, 6 * 60 * 1000);
    verifyLullLog();

    clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(16L).getMillis());
    tracker.reportBundleLull(mockThread, 30 * 60 * 1000);
    verifyLullLog();
  }

  private void verifyLullLog() throws IOException {
    File[] files = logFolder.listFiles();
    assertThat(files, Matchers.arrayWithSize(1));
    File logFile = files[0];
    List<String> lines = Files.readAllLines(logFile.toPath());

    String warnLines =
        Joiner.on("\n").join(Iterables.filter(lines, line -> line.contains("\"WARN\"")));
    assertThat(
        warnLines,
        Matchers.allOf(
            Matchers.containsString("Operation ongoing in bundle for at least"),
            Matchers.containsString(" without completing"),
            Matchers.containsString("Processing times in each step"),
            Matchers.containsString(
                "org.apache.beam.runners.dataflow.worker.DataflowExecutionContext$DataflowExecutionStateTracker"),
            Matchers.containsString("userpackage.SomeUserDoFn.helperMethod"),
            Matchers.not(Matchers.containsString(SimpleDoFnRunner.class.getName()))));

    String infoLines =
        Joiner.on("\n").join(Iterables.filter(lines, line -> line.contains("\"INFO\"")));
    assertThat(
        infoLines,
        Matchers.not(
            Matchers.anyOf(
                Matchers.containsString("Thread[backgroundThread,"),
                Matchers.containsString(
                    "org.apache.beam.runners.dataflow.worker.StackTraceUtil"))));
    // Truncate the file when done to prepare for the next test.
    new FileOutputStream(logFile, false).getChannel().truncate(0).close();
  }

  private DataflowExecutionStateTracker createTracker(Clock clock) {
    return new DataflowExecutionStateTracker(
        sampler,
        new TestDataflowExecutionState(NameContext.forStage("test-stage"), "other"),
        counterSet,
        options,
        "test-work-item-id",
        clock);
  }
}
