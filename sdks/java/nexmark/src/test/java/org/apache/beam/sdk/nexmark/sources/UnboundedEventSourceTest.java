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
package org.apache.beam.sdk.nexmark.sources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorCheckpoint;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test UnboundedEventSource.
 */
@RunWith(JUnit4.class)
public class UnboundedEventSourceTest {
  private GeneratorConfig makeConfig(long n) {
    return new GeneratorConfig(
        NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, n, 0);
  }

  /**
   * Helper for tracking which ids we've seen (so we can detect dups) and
   * confirming reading events match the model events.
   */
  private static class EventIdChecker {
    private final Set<Long> seenPersonIds = new HashSet<>();
    private final Set<Long> seenAuctionIds = new HashSet<>();

    public void add(Event event) {
      if (event.newAuction != null) {
        assertTrue(seenAuctionIds.add(event.newAuction.id));
      } else if (event.newPerson != null) {
        assertTrue(seenPersonIds.add(event.newPerson.id));
      }
    }

    public void add(int n, UnboundedReader<Event> reader, Generator modelGenerator)
        throws IOException {
      for (int i = 0; i < n; i++) {
        assertTrue(modelGenerator.hasNext());
        Event modelEvent = modelGenerator.next().getValue();
        assertTrue(reader.advance());
        Event actualEvent = reader.getCurrent();
        assertEquals(modelEvent.toString(), actualEvent.toString());
        add(actualEvent);
      }
    }
  }

  /**
   * Check aggressively checkpointing and resuming a reader gives us exactly the
   * same event stream as reading directly.
   */
  @Test
  public void resumeFromCheckpoint() throws IOException {
    Random random = new Random(297);
    int n = 47293;
    GeneratorConfig config = makeConfig(n);
    Generator modelGenerator = new Generator(config);

    EventIdChecker checker = new EventIdChecker();
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    UnboundedEventSource source = new UnboundedEventSource(config, 1, 0, false);
    UnboundedReader<Event> reader = source.createReader(options, null);

    while (n > 0) {
      int m = Math.min(459 + random.nextInt(455), n);
      System.out.printf("reading %d...%n", m);
      checker.add(m, reader, modelGenerator);
      n -= m;
      System.out.printf("splitting with %d remaining...%n", n);
      CheckpointMark checkpointMark = reader.getCheckpointMark();
      reader = source.createReader(options, (GeneratorCheckpoint) checkpointMark);
    }

    assertFalse(reader.advance());
  }
}
