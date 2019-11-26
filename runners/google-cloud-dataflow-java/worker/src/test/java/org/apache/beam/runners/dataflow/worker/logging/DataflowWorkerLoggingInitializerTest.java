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
package org.apache.beam.runners.dataflow.worker.logging;

import static org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer.DEFAULT_RUNNER_LOGGING_LOCATION;
import static org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer.DEFAULT_SDK_LOGGING_LOCATION;
import static org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer.RUNNER_FILEPATH_PROPERTY;
import static org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer.SDK_FILEPATH_PROPERTY;
import static org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer.getLoggingHandler;
import static org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer.getSdkLoggingHandler;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.WorkerLogLevelOverrides;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link DataflowWorkerLoggingInitializer}.
 *
 * <p>Tests which validate written log messages should assume that other background tasks may
 * concurrently be writing log messages, since registered log handlers are global. Therefore it is
 * not safe to assert on log counts or whether the retrieved log collection is empty.
 */
@RunWith(JUnit4.class)
public class DataflowWorkerLoggingInitializerTest {
  @Rule public TemporaryFolder logFolder = new TemporaryFolder();

  @Rule public RestoreSystemProperties restoreProperties = new RestoreSystemProperties();

  // Should match {@link DataflowWorkerLoggingInitializer#FILEPATH_PROPERTY}
  private static final String LOGPATH_PROPERTY = "dataflow.worker.logging.filepath";

  @Before
  public void setUp() {
    Path logFileBasePath = Paths.get(logFolder.getRoot().getAbsolutePath(), "logfile.txt");
    System.setProperty(LOGPATH_PROPERTY, logFileBasePath.toString());
    LogManager.getLogManager().reset();
    DataflowWorkerLoggingInitializer.reset();
    DataflowWorkerLoggingInitializer.initialize();
  }

  @After
  public void tearDown() {
    LogManager.getLogManager().reset();
    DataflowWorkerLoggingInitializer.reset();
  }

  @Test
  public void testWithDefaults() {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);

    DataflowWorkerLoggingInitializer.configure(options);

    Logger rootLogger = LogManager.getLogManager().getLogger("");
    assertEquals(1, rootLogger.getHandlers().length);
    assertEquals(Level.INFO, rootLogger.getLevel());
    assertIsDataflowWorkerLoggingHandler(rootLogger.getHandlers()[0], Level.ALL);
  }

  @Test
  public void testWithConfigurationOverride() {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);
    options.setDefaultWorkerLogLevel(DataflowWorkerLoggingOptions.Level.WARN);

    DataflowWorkerLoggingInitializer.configure(options);

    Logger rootLogger = LogManager.getLogManager().getLogger("");
    assertEquals(1, rootLogger.getHandlers().length);
    assertEquals(Level.WARNING, rootLogger.getLevel());
    assertIsDataflowWorkerLoggingHandler(rootLogger.getHandlers()[0], Level.ALL);
  }

  @Test
  public void testWithCustomLogLevels() {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);
    options.setWorkerLogLevelOverrides(
        new WorkerLogLevelOverrides()
            .addOverrideForName("A", DataflowWorkerLoggingOptions.Level.DEBUG)
            .addOverrideForName("B", DataflowWorkerLoggingOptions.Level.ERROR));

    DataflowWorkerLoggingInitializer.configure(options);

    Logger aLogger = LogManager.getLogManager().getLogger("A");
    assertEquals(0, aLogger.getHandlers().length);
    assertEquals(Level.FINE, aLogger.getLevel());
    assertTrue(aLogger.getUseParentHandlers());

    Logger bLogger = LogManager.getLogManager().getLogger("B");
    assertEquals(Level.SEVERE, bLogger.getLevel());
    assertEquals(0, bLogger.getHandlers().length);
    assertTrue(aLogger.getUseParentHandlers());
  }

  private void assertIsDataflowWorkerLoggingHandler(Handler handler, Level level) {
    assertThat(handler, instanceOf(DataflowWorkerLoggingHandler.class));
    assertEquals(level, handler.getLevel());
  }

  @Test
  public void testStrictGlobalFilterAndRelaxedOverride() throws IOException {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);
    options.setDefaultWorkerLogLevel(DataflowWorkerLoggingOptions.Level.ERROR);
    options.setWorkerLogLevelOverrides(
        new WorkerLogLevelOverrides()
            .addOverrideForName("A", DataflowWorkerLoggingOptions.Level.INFO));

    DataflowWorkerLoggingInitializer.configure(options);
    LogManager.getLogManager().getLogger("A").info("foobar");

    verifyLogOutput("foobar");
  }

  @Test
  public void testSystemOutToLogger() throws Throwable {
    System.out.println("afterInitialization");
    verifyLogOutput("afterInitialization");
  }

  @Test
  public void testSystemErrToLogger() throws Throwable {
    System.err.println("afterInitialization");
    verifyLogOutput("afterInitialization");
  }

  @Test
  public void testSystemOutRespectsFilterConfig() throws IOException {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);
    options.setDefaultWorkerLogLevel(DataflowWorkerLoggingOptions.Level.ERROR);
    DataflowWorkerLoggingInitializer.configure(options);

    System.out.println("sys.out");
    System.err.println("sys.err");

    List<String> actualLines = retrieveLogLines();
    assertThat(actualLines, not(hasItem(containsString("sys.out"))));
    assertThat(actualLines, hasItem(containsString("sys.err")));
  }

  @Test
  public void testSystemOutLevelOverrides() throws IOException {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);

    options.setWorkerSystemOutMessageLevel(DataflowWorkerLoggingOptions.Level.WARN);
    DataflowWorkerLoggingInitializer.configure(options.as(DataflowWorkerLoggingOptions.class));

    System.out.println("foobar");
    verifyLogOutput("WARN");
  }

  @Test
  public void testSystemOutCustomLogLevel() throws IOException {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);
    options.setWorkerLogLevelOverrides(
        new WorkerLogLevelOverrides()
            .addOverrideForName("System.out", DataflowWorkerLoggingOptions.Level.ERROR));
    DataflowWorkerLoggingInitializer.configure(options);

    System.out.println("sys.out");

    List<String> actualLines = retrieveLogLines();

    // N.B.: It's not safe to assert that actualLines is "empty" since the logging framework is
    // global and logs may be concurrently written by other infrastructure.
    assertThat(actualLines, not(hasItem(containsString("sys.out"))));
  }

  @Test
  public void testSystemErrLevelOverrides() throws IOException {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);

    options.setWorkerSystemErrMessageLevel(DataflowWorkerLoggingOptions.Level.WARN);
    DataflowWorkerLoggingInitializer.configure(options.as(DataflowWorkerLoggingOptions.class));

    System.err.println("foobar");
    verifyLogOutput("WARN");
  }

  /**
   * Verify we can handle additional user logging configuration. Specifically, ensure that we
   * gracefully handle adding an additional log handler which forwards to stdout.
   */
  @Test
  public void testUserHandlerForwardsStdOut() throws Throwable {
    registerRootLogHandler(new StdOutLogHandler());
    org.slf4j.Logger log = LoggerFactory.getLogger(DataflowWorkerLoggingInitializerTest.class);

    log.info("foobar");
    verifyLogOutput("foobar");
  }

  @Test
  public void testLoggingHandlersAreDifferent() {
    assertThat(getLoggingHandler(), not(getSdkLoggingHandler()));
    assertThat(DEFAULT_RUNNER_LOGGING_LOCATION, not(DEFAULT_SDK_LOGGING_LOCATION));
    assertThat(RUNNER_FILEPATH_PROPERTY, not(SDK_FILEPATH_PROPERTY));
  }

  static class StdOutLogHandler extends java.util.logging.Handler {
    @Override
    public void publish(LogRecord record) {
      System.out.println(record.getMessage());
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }

  private void registerRootLogHandler(Handler handler) {
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.addHandler(handler);
  }

  private void verifyLogOutput(String substring) throws IOException {
    List<String> logLines = retrieveLogLines();
    assertThat(logLines, hasItem(containsString(substring)));
  }

  private List<String> retrieveLogLines() throws IOException {
    List<String> allLogLines = Lists.newArrayList();
    for (File logFile : logFolder.getRoot().listFiles()) {
      allLogLines.addAll(Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8));
    }

    return allLogLines;
  }
}
