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

import static org.apache.beam.runners.core.metrics.ExecutionStateTracker.ABORT_STATE_NAME;
import static org.apache.beam.runners.core.metrics.ExecutionStateTracker.FINISH_STATE_NAME;
import static org.apache.beam.runners.core.metrics.ExecutionStateTracker.PROCESS_STATE_NAME;
import static org.apache.beam.runners.core.metrics.ExecutionStateTracker.START_STATE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.FixedClock;
import com.google.api.client.util.Clock;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext.BatchModeExecutionStateRegistry;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DataflowOperationContext}. */
@RunWith(Enclosed.class)
public class DataflowOperationContextTest {

  /**
   * Tests for the management of {@link DataflowExecutionState} in {@link DataflowOperationContext}.
   */
  @RunWith(JUnit4.class)
  public static class ContextStatesTest {

    @Mock private CounterFactory counterFactory;
    @Mock private MetricsContainer metricsContainer;

    private DataflowExecutionStateRegistry stateRegistry = new BatchModeExecutionStateRegistry();

    @Mock private ScopedProfiler scopedProfiler;

    @Mock private ProfileScope emptyScope;
    @Mock private ProfileScope profileScope;

    private DataflowExecutionState otherState;
    private DataflowExecutionState startState;
    private DataflowExecutionState processState;
    private DataflowExecutionState abortState;

    private ExecutionStateTracker stateTracker = ExecutionStateTracker.newForTest();
    private DataflowOperationContext operationContext;

    @Before
    public void setUp() {
      MockitoAnnotations.initMocks(this);

      otherState = stateRegistry.getState(NameContext.forStage("STAGE"), "other", null, emptyScope);
      startState =
          stateRegistry.getState(
              NameContextsForTests.nameContextForTest(),
              START_STATE_NAME,
              metricsContainer,
              profileScope);
      processState =
          stateRegistry.getState(
              NameContextsForTests.nameContextForTest(),
              PROCESS_STATE_NAME,
              metricsContainer,
              profileScope);
      abortState =
          stateRegistry.getState(
              NameContextsForTests.nameContextForTest(),
              ABORT_STATE_NAME,
              metricsContainer,
              profileScope);
      stateRegistry.getState(
          NameContextsForTests.nameContextForTest(),
          FINISH_STATE_NAME,
          metricsContainer,
          profileScope);

      operationContext =
          new DataflowOperationContext(
              counterFactory,
              NameContextsForTests.nameContextForTest(),
              metricsContainer,
              stateTracker,
              stateRegistry);

      // MapTaskExecutor ensures we start in the "other" (outside of a step) state.
      stateTracker.enterState(otherState);
      Mockito.reset(emptyScope); // reset so tests don't need to expect the activation above
    }

    @Test
    public void enterStart() {
      operationContext.enterStart();

      assertThat(stateTracker.getCurrentState(), sameInstance(startState));
      verify(profileScope).activate();
    }

    @Test
    public void exitStart() throws IOException {
      operationContext.enterStart().close();

      assertThat(stateTracker.getCurrentState(), sameInstance(otherState));
      verify(profileScope).activate();
      verify(emptyScope).activate();
    }

    @Test
    public void enterAllStates() {
      operationContext.enterStart();
      operationContext.enterProcess();
      operationContext.enterFinish();
      operationContext.enterAbort();

      assertThat(stateTracker.getCurrentState(), sameInstance(abortState));
      verify(profileScope, times(4)).activate(); // start, process, finish, abort
    }

    @Test
    public void enterAllStatesAndExit() throws IOException {
      operationContext.enterStart();
      operationContext.enterProcess();
      Closeable finish = operationContext.enterFinish();
      Closeable abort = operationContext.enterAbort();
      abort.close();
      finish.close();

      assertThat(stateTracker.getCurrentState(), sameInstance(processState));
      verify(profileScope, times(6)).activate(); // start, process, finish, abort, finish, process
    }
  }

  /** Tests for the lull logging in {@link DataflowOperationContext}. */
  @RunWith(JUnit4.class)
  public static class LullLoggingTest {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    private File logFolder;

    @Before
    public void setUp() throws IOException {
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

    @Test
    public void testLullReportsRightTrace() throws Exception {
      Thread mockThread = mock(Thread.class);
      FixedClock clock = new FixedClock(Clock.SYSTEM.currentTimeMillis());

      DataflowExecutionState executionState =
          new DataflowExecutionState(
              NameContextsForTests.nameContextForTest(),
              "somestate",
              null /* requestingStepName */,
              null /* inputIndex */,
              null /* metricsContainer */,
              ScopedProfiler.INSTANCE.emptyScope(),
              clock) {
            @Nullable
            @Override
            public CounterUpdate extractUpdate(boolean isFinalUpdate) {
              // not being used for extracting updates
              return null;
            }

            @Override
            public void takeSample(long millisSinceLastSample) {
              // Not being used for sampling
            }
          };

      StackTraceElement[] doFnStackTrace =
          new StackTraceElement[] {
            new StackTraceElement(
                "userpackage.SomeUserDoFn", "helperMethod", "SomeUserDoFn.java", 250),
            new StackTraceElement("userpackage.SomeUserDoFn", "process", "SomeUserDoFn.java", 450),
            new StackTraceElement(
                SimpleDoFnRunner.class.getName(), "processElement", "SimpleDoFnRunner.java", 500),
          };
      when(mockThread.getStackTrace()).thenReturn(doFnStackTrace);

      // Adding test for the full thread dump, but since we can't mock
      // Thread.getAllStackTraces(), we are starting a background thread
      // to verify the full thread dump.
      Thread backgroundThread =
          new Thread("backgroundThread") {
            @Override
            public void run() {
              try {
                Thread.sleep(Long.MAX_VALUE);
              } catch (InterruptedException e) {
                // exiting the thread
              }
            }
          };

      backgroundThread.start();
      try {
        // Full thread dump should be performed, because we never performed
        // a full thread dump before, and the lull duration is more than 20
        // minutes.
        executionState.reportLull(mockThread, 30 * 60 * 1000);
        verifyLullLog(true);

        // Full thread dump should not be performed because the last dump
        // was only 5 minutes ago.
        clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(5L).getMillis());
        executionState.reportLull(mockThread, 30 * 60 * 1000);
        verifyLullLog(false);

        // Full thread dump should not be performed because the lull duration
        // is only 6 minutes.
        clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(16L).getMillis());
        executionState.reportLull(mockThread, 6 * 60 * 1000);
        verifyLullLog(false);

        // Full thread dump should be performed, because it has been 21 minutes
        // since the last dump, and the lull duration is more than 20 minutes.
        clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(16L).getMillis());
        executionState.reportLull(mockThread, 30 * 60 * 1000);
        verifyLullLog(true);
      } finally {
        // Cleaning up the background thread.
        backgroundThread.interrupt();
        backgroundThread.join();
      }
    }

    private void verifyLullLog(boolean hasFullThreadDump) throws IOException {
      File[] files = logFolder.listFiles();
      assertThat(files, Matchers.arrayWithSize(1));
      File logFile = files[0];
      List<String> lines = Files.readAllLines(logFile.toPath());

      String warnLines =
          Joiner.on("\n").join(Iterables.filter(lines, line -> line.contains("\"WARN\"")));
      assertThat(
          warnLines,
          Matchers.allOf(
              Matchers.containsString(
                  "Operation ongoing in step " + NameContextsForTests.USER_NAME),
              Matchers.containsString(" without outputting or completing in state somestate"),
              Matchers.containsString("userpackage.SomeUserDoFn.helperMethod"),
              Matchers.not(Matchers.containsString(SimpleDoFnRunner.class.getName()))));

      String infoLines =
          Joiner.on("\n").join(Iterables.filter(lines, line -> line.contains("\"INFO\"")));
      if (hasFullThreadDump) {
        assertThat(
            infoLines,
            Matchers.allOf(
                Matchers.containsString("Thread[backgroundThread,"),
                Matchers.containsString(
                    "org.apache.beam.runners.dataflow.worker.DataflowOperationContext"),
                Matchers.not(Matchers.containsString(SimpleDoFnRunner.class.getName()))));
      } else {
        assertThat(
            infoLines,
            Matchers.not(
                Matchers.anyOf(
                    Matchers.containsString("Thread[backgroundThread,"),
                    Matchers.containsString(
                        "org.apache.beam.runners.dataflow.worker.DataflowOperationContext"))));
      }
      // Truncate the file when done to prepare for the next test.
      new FileOutputStream(logFile, false).getChannel().truncate(0).close();
    }

    @Test
    public void testDurationFormatting() {
      assertThat(
          DataflowOperationContext.formatDuration(getDuration(0, 0, 5, 10, 500)),
          equalTo("05m10s"));
      assertThat(
          DataflowOperationContext.formatDuration(getDuration(0, 2, 10, 23, 500)),
          equalTo("02h10m23s"));
      assertThat(
          DataflowOperationContext.formatDuration(getDuration(1, 0, 0, 23, 500)),
          equalTo("24h00m23s"));
      assertThat(
          DataflowOperationContext.formatDuration(getDuration(1, 0, 10, 23, 500)),
          equalTo("24h10m23s"));
      assertThat(
          DataflowOperationContext.formatDuration(getDuration(2, 0, 0, 23, 500)),
          equalTo("48h00m23s"));
    }

    private Duration getDuration(int days, int hours, int minutes, int seconds, int millis) {
      return Duration.standardDays(days)
          .plus(Duration.standardHours(hours))
          .plus(Duration.standardMinutes(minutes))
          .plus(Duration.standardSeconds(seconds))
          .plus(Duration.millis(millis));
    }
  }
}
