/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.WorkerLogLevelOverrides;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/** Unit tests for {@link DataflowWorkerLoggingInitializer}. */
@RunWith(JUnit4.class)
public class DataflowWorkerLoggingInitializerTest {
  @After
  public void tearDown() {
    LogManager.getLogManager().reset();
    DataflowWorkerLoggingInitializer.reset();
  }

  @Test
  public void testWithDefaults() {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);

    DataflowWorkerLoggingInitializer.initialize();
    DataflowWorkerLoggingInitializer.configure(options);

    Logger rootLogger = LogManager.getLogManager().getLogger("");
    assertEquals(1, rootLogger.getHandlers().length);
    assertEquals(Level.INFO, rootLogger.getLevel());
    assertTrue(isDataflowWorkerLoggingHandler(rootLogger.getHandlers()[0], Level.ALL));
  }

  @Test
  public void testWithConfigurationOverride() {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);
    options.setDefaultWorkerLogLevel(DataflowWorkerLoggingOptions.Level.WARN);

    DataflowWorkerLoggingInitializer.initialize();
    DataflowWorkerLoggingInitializer.configure(options);

    Logger rootLogger = LogManager.getLogManager().getLogger("");
    assertEquals(1, rootLogger.getHandlers().length);
    assertEquals(Level.WARNING, rootLogger.getLevel());
    assertTrue(isDataflowWorkerLoggingHandler(rootLogger.getHandlers()[0], Level.ALL));
  }

  @Test
  public void testWithCustomLogLevels() {
    DataflowWorkerLoggingOptions options =
        PipelineOptionsFactory.as(DataflowWorkerLoggingOptions.class);
    options.setWorkerLogLevelOverrides(new WorkerLogLevelOverrides()
        .addOverrideForName("A", DataflowWorkerLoggingOptions.Level.DEBUG)
        .addOverrideForName("B", DataflowWorkerLoggingOptions.Level.ERROR));

    DataflowWorkerLoggingInitializer.initialize();
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

  private boolean isDataflowWorkerLoggingHandler(Handler handler, Level level) {
    return handler instanceof DataflowWorkerLoggingHandler && level.equals(handler.getLevel());
  }
}
