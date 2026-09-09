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
package org.apache.beam.sdk.extensions.openlineage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that the terminal event is never lost.
 *
 * <p>Two threads can observe completion: the periodic {@link OpenLineageJobTracker} daemon and
 * whichever thread calls {@code waitUntilFinish()} or {@code cancel()}. Only one of them may emit,
 * but the run must not be marked finished until the transport has actually taken the event, or a
 * daemon killed by JVM exit part way through emission would leave the run open forever.
 */
@RunWith(JUnit4.class)
public class OpenLineageTerminalEventTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File eventsFile;

  @Before
  public void setUp() throws Exception {
    eventsFile = new File(temporaryFolder.getRoot(), "events.jsonl");
    RejectingTransportConfig transportConfig = new RejectingTransportConfig();
    transportConfig.setLocation(eventsFile.getAbsolutePath());
    BeamOpenLineageConfig config = new BeamOpenLineageConfig();
    config.setTransportConfig(transportConfig);
    OpenLineageContext.resetForTests();
    OpenLineageContext.overrideConfigForTests(config);
    RejectingTransport.reset();
  }

  @After
  public void tearDown() {
    OpenLineageContext.resetForTests();
    RejectingTransport.reset();
  }

  private OpenLineageContext context() {
    PipelineOptions options = PipelineOptionsFactory.create();
    OpenLineagePipelineOptions olOptions = options.as(OpenLineagePipelineOptions.class);
    olOptions.setOpenLineageNamespace("test_namespace");
    olOptions.setOpenLineageJobName("terminal_event_job");
    return OpenLineageContext.getOrCreate(options);
  }

  private List<OpenLineage.RunEvent> readEvents() throws Exception {
    if (!eventsFile.exists()) {
      return new ArrayList<>();
    }
    List<OpenLineage.RunEvent> events = new ArrayList<>();
    for (String line : Files.readAllLines(eventsFile.toPath(), StandardCharsets.UTF_8)) {
      if (!line.trim().isEmpty()) {
        events.add(OpenLineageClientUtils.runEventFromJson(line));
      }
    }
    return events;
  }

  private long countOf(
      List<OpenLineage.RunEvent> events, OpenLineage.RunEvent.EventType eventType) {
    return events.stream().filter(e -> e.getEventType() == eventType).count();
  }

  @Test
  public void terminalEventIsRetriedWhenTheTransportRejectsIt() throws Exception {
    OpenLineageContext context = context();
    context.onJobSubmitted();
    assertEquals(1, countOf(readEvents(), OpenLineage.RunEvent.EventType.START));

    // The backend is briefly unavailable, so the first completion signal cannot deliver.
    RejectingTransport.rejectNext(1);
    context.onJobFinished(OpenLineage.RunEvent.EventType.COMPLETE, null);
    assertEquals(
        "a rejected terminal event must not be recorded",
        0,
        countOf(readEvents(), OpenLineage.RunEvent.EventType.COMPLETE));

    // A later completion signal must retry it rather than treating the run as settled.
    context.onJobFinished(OpenLineage.RunEvent.EventType.COMPLETE, null);
    assertEquals(
        "the retried terminal event must reach the transport",
        1,
        countOf(readEvents(), OpenLineage.RunEvent.EventType.COMPLETE));
  }

  @Test
  public void repeatedCompletionSignalsEmitTheTerminalEventExactlyOnce() throws Exception {
    OpenLineageContext context = context();
    context.onJobSubmitted();

    context.onJobFinished(OpenLineage.RunEvent.EventType.COMPLETE, null);
    context.onJobFinished(OpenLineage.RunEvent.EventType.COMPLETE, null);
    context.onJobFinished(OpenLineage.RunEvent.EventType.ABORT, null);

    List<OpenLineage.RunEvent> events = readEvents();
    assertEquals(1, countOf(events, OpenLineage.RunEvent.EventType.COMPLETE));
    assertEquals(0, countOf(events, OpenLineage.RunEvent.EventType.ABORT));
  }

  @Test
  public void concurrentCompletionSignalsEmitTheTerminalEventExactlyOnce() throws Exception {
    OpenLineageContext context = context();
    context.onJobSubmitted();

    int threads = 8;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    List<Thread> workers = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      Thread worker =
          new Thread(
              () -> {
                ready.countDown();
                try {
                  go.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                context.onJobFinished(OpenLineage.RunEvent.EventType.COMPLETE, null);
              });
      workers.add(worker);
      worker.start();
    }
    assertTrue(ready.await(10, TimeUnit.SECONDS));
    go.countDown();
    for (Thread worker : workers) {
      worker.join(10_000);
    }

    List<OpenLineage.RunEvent> events = readEvents();
    assertEquals(
        "exactly one terminal event, no matter how many threads observe completion",
        1,
        countOf(events, OpenLineage.RunEvent.EventType.COMPLETE));
  }

  @Test
  public void runningEventsStopOnceTheRunIsFinished() throws Exception {
    OpenLineageContext context = context();
    context.onJobSubmitted();
    context.onJobFinished(OpenLineage.RunEvent.EventType.COMPLETE, null);

    context.onTrackingTick();

    List<OpenLineage.RunEvent> events = readEvents();
    assertEquals(
        "no RUNNING event may follow the terminal event",
        0,
        countOf(events, OpenLineage.RunEvent.EventType.RUNNING));
    assertEquals(1, countOf(events, OpenLineage.RunEvent.EventType.COMPLETE));
  }
}
