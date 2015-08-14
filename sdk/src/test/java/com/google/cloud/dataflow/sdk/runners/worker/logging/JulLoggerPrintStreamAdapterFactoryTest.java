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

import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.PrintStream;
import java.util.logging.Level;

/** Tests for {@link JulLoggerPrintStreamAdapterFactory}. */
@RunWith(JUnit4.class)
public class JulLoggerPrintStreamAdapterFactoryTest {
  private static final String NAME = "test";
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(NAME);

  @Test
  public void testLogOnNewLine() {
    PrintStream printStream = JulLoggerPrintStreamAdapterFactory.create(NAME, Level.INFO);
    printStream.println("blah");
    expectedLogs.verifyInfo("blah");
  }

  @Test
  public void testLogOnlyUptoNewLine() {
    PrintStream printStream = JulLoggerPrintStreamAdapterFactory.create(NAME, Level.INFO);
    printStream.println("blah");
    printStream.print("foo");
    expectedLogs.verifyInfo("blah");
    expectedLogs.verifyNotLogged("foo");
  }

  @Test
  public void testLogMultiLine() {
    PrintStream printStream = JulLoggerPrintStreamAdapterFactory.create(NAME, Level.INFO);
    printStream.format("blah%nfoo%n");
    expectedLogs.verifyInfo("blah");
    expectedLogs.verifyInfo("foo");
  }

  @Test
  public void testDontLogIfNoNewLine() {
    PrintStream printStream = JulLoggerPrintStreamAdapterFactory.create(NAME, Level.INFO);
    printStream.print("blah");
    expectedLogs.verifyNotLogged("blah");
  }

  @Test
  public void testLogOnFlush() {
    PrintStream printStream = JulLoggerPrintStreamAdapterFactory.create(NAME, Level.INFO);
    printStream.print("blah");
    printStream.flush();
    expectedLogs.verifyInfo("blah");
  }

  @Test
  public void testLogOnClose() {
    PrintStream printStream = JulLoggerPrintStreamAdapterFactory.create(NAME, Level.INFO);
    printStream.print("blah");
    printStream.close();
    expectedLogs.verifyInfo("blah");
  }
}

