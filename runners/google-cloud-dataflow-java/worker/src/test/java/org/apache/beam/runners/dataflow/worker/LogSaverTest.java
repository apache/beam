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

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LogSaver}. */
@RunWith(JUnit4.class)
public class LogSaverTest {
  @Test
  public void testInitialization() {
    LogSaver saver = new LogSaver();
    assertThat(saver.getLogs(), empty());
  }

  @Test
  public void testPublishedLogsAreSaved() {
    int numLogsToPublish = 10;

    LogSaver saver = new LogSaver();
    Set<LogRecord> records = Sets.newHashSet();
    for (int i = 0; i < numLogsToPublish; i++) {
      LogRecord record = new LogRecord(Level.WARNING, "message #" + i);
      records.add(record);
      saver.publish(record);
    }

    assertEquals(records, Sets.newHashSet(saver.getLogs()));
  }

  @Test
  public void testJulIntegration() {
    Logger log = Logger.getLogger("any.logger");
    LogSaver saver = new LogSaver();
    log.addHandler(saver);

    log.warning("foobar");
    assertThat(saver.getLogs(), Matchers.hasItem(LogRecordMatcher.hasLog(Level.WARNING, "foobar")));
  }
}
