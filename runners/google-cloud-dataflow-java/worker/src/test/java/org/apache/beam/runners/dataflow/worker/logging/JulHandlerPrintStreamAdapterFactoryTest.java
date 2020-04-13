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

import static org.apache.beam.runners.dataflow.worker.LogRecordMatcher.hasLogItem;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.apache.beam.runners.dataflow.worker.LogSaver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JulHandlerPrintStreamAdapterFactory}. */
@RunWith(JUnit4.class)
public class JulHandlerPrintStreamAdapterFactoryTest {
  private static final String LOGGER_NAME = "test";

  private LogSaver handler;

  @Before
  public void setUp() {
    JulHandlerPrintStreamAdapterFactory.reset();
    handler = new LogSaver();
  }

  @Test
  public void testLogWarningOnFirstUsage() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.println("partial");
    assertThat(
        handler.getLogs(),
        hasLogItem(Level.WARNING, "Please use a logger instead of System.out or System.err"));
  }

  @Test
  public void testLogOnNewLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.println("blah");
    assertThat(handler.getLogs(), hasLogItem(Level.INFO, "blah"));
  }

  @Test
  public void testLogRecordMetadata() {
    PrintStream printStream =
        JulHandlerPrintStreamAdapterFactory.create(handler, "fooLogger", Level.WARNING);
    printStream.println("anyMessage");

    assertThat(handler.getLogs(), not(empty()));
    LogRecord log = Iterables.get(handler.getLogs(), 0);

    assertThat(log.getLevel(), is(Level.WARNING));
    assertThat(log.getLoggerName(), is("fooLogger"));
  }

  @Test
  public void testLogOnlyUptoNewLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.println("blah");
    printStream.print("foo");
    assertThat(handler.getLogs(), hasLogItem("blah"));
    assertThat(handler.getLogs(), not(hasLogItem("foo")));
  }

  @Test
  public void testLogMultiLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.format("blah%nfoo%n");
    assertThat(handler.getLogs(), hasLogItem("blah"));
    assertThat(handler.getLogs(), hasLogItem("foo"));
  }

  @Test
  public void testDontLogIfNoNewLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.print("blah");
    assertThat(handler.getLogs(), not(hasLogItem("blah")));
  }

  @Test
  public void testLogOnFlush() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.print("blah");
    printStream.flush();
    assertThat(handler.getLogs(), hasLogItem("blah"));
  }

  @Test
  public void testLogOnClose() {
    try (PrintStream printStream = createPrintStreamAdapter()) {
      printStream.print("blah");
    }
    assertThat(handler.getLogs(), hasLogItem("blah"));
  }

  @Test
  public void testLogRawBytes() {
    PrintStream printStream = createPrintStreamAdapter();
    String msg = "♠ ♡ ♢ ♣ ♤ ♥ ♦ ♧";
    byte[] bytes = msg.getBytes(Charset.defaultCharset());
    printStream.write(bytes, 0, 1);
    printStream.write(bytes, 1, 4);
    printStream.write(bytes, 5, 15);
    printStream.write(bytes, 20, bytes.length - 20);
    assertThat(handler.getLogs(), is(empty()));
    String newlineMsg = "♠ ♡ \n♦ ♧";
    byte[] newlineMsgBytes = newlineMsg.getBytes(Charset.defaultCharset());
    printStream.write(newlineMsgBytes, 0, newlineMsgBytes.length);
    assertThat(handler.getLogs(), hasLogItem(msg + newlineMsg));
  }

  @Test
  public void testNoEmptyMessages() {
    try (PrintStream printStream = createPrintStreamAdapter()) {
      printStream.println("blah");
      printStream.print("\n");
      printStream.flush();
      printStream.println("");
      printStream.flush();
      printStream.print("");
      printStream.flush();
      byte[] bytes = "a".getBytes(Charset.defaultCharset());
      printStream.write(bytes, 0, 0);
      printStream.flush();
    }

    for (LogRecord log : handler.getLogs()) {
      assertThat(log.getMessage(), not(blankOrNullString()));
    }
  }

  private PrintStream createPrintStreamAdapter() {
    return JulHandlerPrintStreamAdapterFactory.create(handler, LOGGER_NAME, Level.INFO);
  }
}
