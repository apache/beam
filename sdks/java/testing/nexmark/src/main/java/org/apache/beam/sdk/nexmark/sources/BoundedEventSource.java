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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** A custom, bounded source of event records. */
public class BoundedEventSource extends BoundedSource<Event> {
  /** Configuration we generate events against. */
  private final GeneratorConfig config;

  /** How many bounded sources to create. */
  private final int numEventGenerators;

  public BoundedEventSource(GeneratorConfig config, int numEventGenerators) {
    this.config = config;
    this.numEventGenerators = numEventGenerators;
  }

  /** A reader to pull events from the generator. */
  private static class EventReader extends BoundedReader<Event> {
    /**
     * Event source we purporting to be reading from. (We can't use Java's capture-outer-class
     * pointer since we must update this field on calls to splitAtFraction.)
     */
    private BoundedEventSource source;

    /** Generator we are reading from. */
    private final Generator generator;

    private boolean reportedStop;

    private @Nullable TimestampedValue<Event> currentEvent;

    public EventReader(BoundedEventSource source, GeneratorConfig config) {
      this.source = source;
      generator = new Generator(config);
      reportedStop = false;
    }

    @Override
    public synchronized boolean start() {
      NexmarkUtils.info("starting bounded generator %s", generator);
      return advance();
    }

    @Override
    public synchronized boolean advance() {
      if (!generator.hasNext()) {
        // No more events.
        if (!reportedStop) {
          reportedStop = true;
          NexmarkUtils.info("stopped bounded generator %s", generator);
        }
        return false;
      }
      currentEvent = generator.next();
      return true;
    }

    @Override
    public synchronized Event getCurrent() throws NoSuchElementException {
      if (currentEvent == null) {
        throw new NoSuchElementException();
      }
      return currentEvent.getValue();
    }

    @Override
    public synchronized Instant getCurrentTimestamp() throws NoSuchElementException {
      if (currentEvent == null) {
        throw new NoSuchElementException();
      }
      return currentEvent.getTimestamp();
    }

    @Override
    public void close() throws IOException {
      // Nothing to close.
    }

    @Override
    public synchronized Double getFractionConsumed() {
      return generator.getFractionConsumed();
    }

    @Override
    public synchronized BoundedSource<Event> getCurrentSource() {
      return source;
    }

    @Override
    public @Nullable synchronized BoundedEventSource splitAtFraction(double fraction) {
      long startId = generator.getCurrentConfig().getStartEventId();
      long stopId = generator.getCurrentConfig().getStopEventId();
      long size = stopId - startId;
      long splitEventId = startId + Math.min((int) (size * fraction), size);
      if (splitEventId <= generator.getNextEventId() || splitEventId == stopId) {
        // Already passed this position or split results in left or right being empty.
        NexmarkUtils.info("split failed for bounded generator %s at %f", generator, fraction);
        return null;
      }

      NexmarkUtils.info("about to split bounded generator %s at %d", generator, splitEventId);

      // Scale back the event space of the current generator, and return a generator config
      // representing the event space we just 'stole' from the current generator.
      GeneratorConfig remainingConfig = generator.splitAtEventId(splitEventId);

      NexmarkUtils.info("split bounded generator into %s and %s", generator, remainingConfig);

      // At this point
      //   generator.events() ++ new Generator(remainingConfig).events()
      //   == originalGenerator.events()

      // We need a new source to represent the now smaller key space for this reader, so
      // that we can maintain the invariant that
      //   this.getCurrentSource().createReader(...)
      // will yield the same output as this.
      source = new BoundedEventSource(generator.getCurrentConfig(), source.numEventGenerators);

      // Return a source from which we may read the 'stolen' event space.
      return new BoundedEventSource(remainingConfig, source.numEventGenerators);
    }
  }

  @Override
  public List<BoundedEventSource> split(long desiredBundleSizeBytes, PipelineOptions options) {
    NexmarkUtils.info("slitting bounded source %s into %d sub-sources", config, numEventGenerators);
    List<BoundedEventSource> results = new ArrayList<>();
    // Ignore desiredBundleSizeBytes and use numEventGenerators instead.
    for (GeneratorConfig subConfig : config.split(numEventGenerators)) {
      results.add(new BoundedEventSource(subConfig, 1));
    }
    return results;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    return config.getEstimatedSizeBytes();
  }

  @Override
  public EventReader createReader(PipelineOptions options) {
    NexmarkUtils.info("creating initial bounded reader for %s", config);
    return new EventReader(this, config);
  }

  @Override
  public void validate() {
    // Nothing to validate.
  }

  @Override
  public Coder<Event> getDefaultOutputCoder() {
    return Event.CODER;
  }
}
