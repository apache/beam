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
package org.apache.beam.sdk.extensions.sql.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.ConsoleHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HeldLogs}. */
@RunWith(JUnit4.class)
public class HeldLogsTest {
  /** A console handler that records what it would print. */
  private static class RecordingConsole extends ConsoleHandler {
    final List<String> messages = new CopyOnWriteArrayList<>();

    @Override
    public synchronized void publish(LogRecord record) {
      messages.add(record.getMessage());
    }
  }

  @Test
  public void holdsBackConsoleLogsUntilClosed() {
    Logger root = Logger.getLogger("");
    Logger logger = Logger.getLogger(HeldLogsTest.class.getName());
    RecordingConsole console = new RecordingConsole();
    root.addHandler(console);
    try {
      HeldLogs logs = HeldLogs.hold();
      logger.info("polling");
      logger.warning("slow poll");
      // Nothing reaches the console while the logs are held.
      assertEquals(ImmutableList.of(), console.messages);
      assertEquals(1, logs.getHiddenCount());

      logs.close();
      // Warnings come through once the logs are let go, and the console works as before.
      logger.info("done");
      assertEquals(ImmutableList.of("slow poll", "done"), console.messages);
    } finally {
      root.removeHandler(console);
    }
  }
}
