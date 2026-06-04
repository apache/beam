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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.event.Level;

/** Tests for {@link LogElements}. */
@RunWith(JUnit4.class)
public class LogElementsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(LogElements.class);

  @Test
  @Category(NeedsRunner.class)
  public void testLogElementsPreservesElements() {
    List<String> elements = Arrays.asList("a", "b", "c");

    PCollection<String> output =
        pipeline.apply(Create.of(elements)).apply(LogElements.<String>info());

    PAssert.that(output).containsInAnyOrder(elements);
    pipeline.run();
  }

  @Test
  public void testFormatForLoggingIncludesConfiguredMetadata() {
    Instant timestamp = new Instant(0);
    IntervalWindow window = new IntervalWindow(timestamp, Duration.standardMinutes(1));

    String message =
        LogElements.formatForLogging(
            "a", "row: ", true, true, true, timestamp, window, PaneInfo.NO_FIRING);

    assertThat(message, containsString("row: a"));
    assertThat(message, containsString("timestamp=1970-01-01T00:00:00.000Z"));
    assertThat(
        message, containsString("window=[1970-01-01T00:00:00.000Z..1970-01-01T00:01:00.000Z)"));
    assertThat(message, containsString("paneInfo=PaneInfo.NO_FIRING"));
  }

  @Test
  public void testLogElementsLogsAtConfiguredLevels() {
    LogElements.log(Level.TRACE, "trace: trace-element");
    LogElements.log(Level.DEBUG, "debug: debug-element");
    LogElements.log(Level.INFO, "info: info-element");
    LogElements.log(Level.WARN, "warn: warn-element");
    LogElements.log(Level.ERROR, "error: error-element");

    expectedLogs.verifyTrace("trace: trace-element");
    expectedLogs.verifyDebug("debug: debug-element");
    expectedLogs.verifyInfo("info: info-element");
    expectedLogs.verifyWarn("warn: warn-element");
    expectedLogs.verifyError("error: error-element");
  }

  @Test
  public void testDisplayData() {
    DisplayData displayData =
        DisplayData.from(
            LogElements.of(Level.WARN)
                .withPrefix("row: ")
                .withTimestamp()
                .withWindow()
                .withPaneInfo());

    assertThat(displayData, hasDisplayItem("level", "WARN"));
    assertThat(displayData, hasDisplayItem("prefix", "row: "));
    assertThat(displayData, hasDisplayItem("withTimestamp", true));
    assertThat(displayData, hasDisplayItem("withWindow", true));
    assertThat(displayData, hasDisplayItem("withPaneInfo", true));
  }
}
