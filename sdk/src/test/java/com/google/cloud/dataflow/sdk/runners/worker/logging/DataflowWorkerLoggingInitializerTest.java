/*
 * Copyright (C) 2014 Google Inc.
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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.testing.RestoreSystemProperties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/** Unit tests for {@link DataflowWorkerLoggingInitializer}. */
@RunWith(JUnit4.class)
public class DataflowWorkerLoggingInitializerTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();

  @Mock LogManager mockLogManager;
  @Mock Logger mockRootLogger;
  @Mock Handler mockHandler;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockLogManager.getLogger("")).thenReturn(mockRootLogger);
    when(mockRootLogger.getHandlers()).thenReturn(new Handler[]{ mockHandler });
  }

  @Test
  public void testWithDefaults() {
    ArgumentCaptor<Handler> argument = ArgumentCaptor.forClass(Handler.class);

    new DataflowWorkerLoggingInitializer().initialize(mockLogManager);
    verify(mockLogManager).getLogger("");
    verify(mockLogManager).reset();
    verify(mockRootLogger).getHandlers();
    verify(mockRootLogger).removeHandler(mockHandler);
    verify(mockRootLogger).setLevel(Level.INFO);
    verify(mockRootLogger, times(2)).addHandler(argument.capture());
    verifyNoMoreInteractions(mockLogManager, mockRootLogger);

    List<Handler> handlers = argument.getAllValues();
    assertTrue(isConsoleHandler(handlers.get(0), Level.INFO));
    assertTrue(isFileHandler(handlers.get(1), Level.INFO));
  }

  @Test
  public void testWithOverrides() {
    ArgumentCaptor<Handler> argument = ArgumentCaptor.forClass(Handler.class);
    System.setProperty("dataflow.worker.logging.level", "WARNING");

    new DataflowWorkerLoggingInitializer().initialize(mockLogManager);
    verify(mockLogManager).getLogger("");
    verify(mockLogManager).reset();
    verify(mockRootLogger).getHandlers();
    verify(mockRootLogger).removeHandler(mockHandler);
    verify(mockRootLogger).setLevel(Level.WARNING);
    verify(mockRootLogger, times(2)).addHandler(argument.capture());
    verifyNoMoreInteractions(mockLogManager, mockRootLogger);

    List<Handler> handlers = argument.getAllValues();
    assertTrue(isConsoleHandler(handlers.get(0), Level.WARNING));
    assertTrue(isFileHandler(handlers.get(1), Level.WARNING));
  }

  private boolean isConsoleHandler(Handler handler, Level level) {
    return handler instanceof ConsoleHandler
        && level.equals(handler.getLevel())
        && handler.getFormatter() instanceof DataflowWorkerLoggingFormatter;
  }

  private boolean isFileHandler(Handler handler, Level level) {
    return handler instanceof FileHandler
        && level.equals(handler.getLevel())
        && handler.getFormatter() instanceof DataflowWorkerLoggingFormatter;
  }
}
