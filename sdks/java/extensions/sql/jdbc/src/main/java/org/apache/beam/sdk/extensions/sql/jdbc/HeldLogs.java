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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Holds back console logging while the live view is on screen, so log lines don't draw over it.
 *
 * <p>In the shell, pipelines log through java.util.logging, and its console handler writes to the
 * same terminal as the view. While held, the root logger's console handlers are swapped for one
 * that keeps warnings and errors and only counts the rest. Closing puts the console handlers back
 * and passes them the warnings and errors that came in.
 */
final class HeldLogs implements AutoCloseable {
  /** How many warnings and errors to keep. Older ones are counted with the rest. */
  private static final int MAX_HELD = 100;

  private final Logger root = Logger.getLogger("");
  private final List<Handler> consoles = new ArrayList<>();
  private final Deque<LogRecord> held = new ArrayDeque<>();
  private int hidden;

  private final Handler holder =
      new Handler() {
        @Override
        public void publish(LogRecord record) {
          synchronized (held) {
            if (record.getLevel().intValue() < Level.WARNING.intValue()) {
              hidden++;
              return;
            }
            held.addLast(record);
            if (held.size() > MAX_HELD) {
              held.removeFirst();
              hidden++;
            }
          }
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
      };

  private HeldLogs() {
    for (Handler handler : root.getHandlers()) {
      if (handler instanceof ConsoleHandler) {
        consoles.add(handler);
        root.removeHandler(handler);
      }
    }
    if (!consoles.isEmpty()) {
      root.addHandler(holder);
    }
  }

  /** Starts holding back what the root logger writes to the console. */
  static HeldLogs hold() {
    return new HeldLogs();
  }

  /** How many log messages were held back and won't be shown. */
  int getHiddenCount() {
    synchronized (held) {
      return hidden;
    }
  }

  /** Puts the console handlers back and passes them the warnings and errors that came in. */
  @Override
  public void close() {
    if (consoles.isEmpty()) {
      return;
    }
    root.removeHandler(holder);
    List<LogRecord> records;
    synchronized (held) {
      records = new ArrayList<>(held);
      held.clear();
    }
    for (Handler console : consoles) {
      root.addHandler(console);
      for (LogRecord record : records) {
        console.publish(record);
      }
      console.flush();
    }
    consoles.clear();
  }
}
