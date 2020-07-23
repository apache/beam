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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorCheckpoint;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom, unbounded source of event records.
 *
 * <p>If {@code isRateLimited} is true, events become available for return from the reader such that
 * the overall rate respect the {@code interEventDelayUs} period if possible. Otherwise, events are
 * returned every time the system asks for one.
 */
public class UnboundedEventSource extends UnboundedSource<Event, GeneratorCheckpoint> {
  private static final Duration BACKLOG_PERIOD = Duration.standardSeconds(30);
  private static final Logger LOG = LoggerFactory.getLogger(UnboundedEventSource.class);

  /** Configuration for generator to use when reading synthetic events. May be split. */
  private final GeneratorConfig config;

  /** How many unbounded sources to create. */
  private final int numEventGenerators;

  /** How many seconds to hold back the watermark. */
  private final long watermarkHoldbackSec;

  /** Are we rate limiting the events? */
  private final boolean isRateLimited;

  public UnboundedEventSource(
      GeneratorConfig config,
      int numEventGenerators,
      long watermarkHoldbackSec,
      boolean isRateLimited) {
    this.config = config;
    this.numEventGenerators = numEventGenerators;
    this.watermarkHoldbackSec = watermarkHoldbackSec;
    this.isRateLimited = isRateLimited;
  }

  /** A reader to pull events from the generator. */
  private class EventReader extends UnboundedReader<Event> {
    /** Generator we are reading from. */
    private final Generator generator;

    /**
     * Current watermark (ms since epoch). Initially set to beginning of time. Then updated to be
     * the time of the next generated event. Then, once all events have been generated, set to the
     * end of time.
     */
    private long watermark;

    /**
     * Current backlog (ms), as delay between timestamp of last returned event and the timestamp we
     * should be up to according to wall-clock time. Used only for logging.
     */
    private long backlogDurationMs;

    /**
     * Current backlog, as estimated number of event bytes we are behind, or null if unknown.
     * Reported to callers.
     */
    private @Nullable Long backlogBytes;

    /** Wallclock time (ms since epoch) we last reported the backlog, or -1 if never reported. */
    private long lastReportedBacklogWallclock;

    /**
     * Event time (ms since epoch) of pending event at last reported backlog, or -1 if never
     * calculated.
     */
    private long timestampAtLastReportedBacklogMs;

    /** Next event to make 'current' when wallclock time has advanced sufficiently. */
    private @Nullable TimestampedValue<Event> pendingEvent;

    /** Wallclock time when {@link #pendingEvent} is due, or -1 if no pending event. */
    private long pendingEventWallclockTime;

    /** Current event to return from getCurrent. */
    private @Nullable TimestampedValue<Event> currentEvent;

    /** Events which have been held back so as to force them to be late. */
    private final Queue<Generator.NextEvent> heldBackEvents = new PriorityQueue<>();

    public EventReader(Generator generator) {
      this.generator = generator;
      watermark = NexmarkUtils.BEGINNING_OF_TIME.getMillis();
      lastReportedBacklogWallclock = -1;
      pendingEventWallclockTime = -1;
      timestampAtLastReportedBacklogMs = -1;
    }

    public EventReader(GeneratorConfig config) {
      this(new Generator(config));
    }

    @Override
    public boolean start() {
      LOG.trace("starting unbounded generator {}", generator);
      return advance();
    }

    @Override
    public boolean advance() {
      long now = System.currentTimeMillis();

      while (pendingEvent == null) {
        if (!generator.hasNext() && heldBackEvents.isEmpty()) {
          // No more events, EVER.
          if (isRateLimited) {
            updateBacklog(System.currentTimeMillis(), 0);
          }
          if (watermark < BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
            watermark = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
            LOG.trace("stopped unbounded generator {}", generator);
          }
          return false;
        }

        Generator.NextEvent next = heldBackEvents.peek();
        if (next != null && next.wallclockTimestamp <= now) {
          // Time to use the held-back event.
          heldBackEvents.poll();
          LOG.debug(
              "replaying held-back event {}ms behind watermark", watermark - next.eventTimestamp);
        } else if (generator.hasNext()) {
          next = generator.nextEvent();
          if (isRateLimited
              && config.getProbDelayedEvent() > 0.0
              && config.getOccasionalDelaySec() > 0
              && ThreadLocalRandom.current().nextDouble() < config.getProbDelayedEvent()) {
            // We'll hold back this event and go around again.
            long delayMs =
                ThreadLocalRandom.current().nextLong(config.getOccasionalDelaySec() * 1000) + 1L;
            LOG.debug("delaying event by {}ms", delayMs);
            heldBackEvents.add(next.withDelay(delayMs));
            continue;
          }
        } else {
          // Waiting for held-back event to fire.
          if (isRateLimited) {
            updateBacklog(now, 0);
          }
          return false;
        }

        pendingEventWallclockTime = next.wallclockTimestamp;
        pendingEvent = TimestampedValue.of(next.event, new Instant(next.eventTimestamp));
        long newWatermark =
            next.watermark - Duration.standardSeconds(watermarkHoldbackSec).getMillis();
        if (newWatermark > watermark) {
          watermark = newWatermark;
        }
      }

      if (isRateLimited) {
        if (pendingEventWallclockTime > now) {
          // We want this event to fire in the future. Try again later.
          updateBacklog(now, 0);
          return false;
        }
        updateBacklog(now, now - pendingEventWallclockTime);
      }

      // This event is ready to fire.
      currentEvent = pendingEvent;
      pendingEvent = null;
      return true;
    }

    private void updateBacklog(long now, long newBacklogDurationMs) {
      backlogDurationMs = newBacklogDurationMs;
      long interEventDelayUs = generator.currentInterEventDelayUs();
      if (interEventDelayUs != 0) {
        long backlogEvents = (backlogDurationMs * 1000 + interEventDelayUs - 1) / interEventDelayUs;
        backlogBytes = generator.getCurrentConfig().estimatedBytesForEvents(backlogEvents);
      }
      if (lastReportedBacklogWallclock < 0
          || now - lastReportedBacklogWallclock > BACKLOG_PERIOD.getMillis()) {
        double timeDialation = Double.NaN;
        if (pendingEvent != null
            && lastReportedBacklogWallclock >= 0
            && timestampAtLastReportedBacklogMs >= 0) {
          long wallclockProgressionMs = now - lastReportedBacklogWallclock;
          long eventTimeProgressionMs =
              pendingEvent.getTimestamp().getMillis() - timestampAtLastReportedBacklogMs;
          timeDialation = (double) eventTimeProgressionMs / (double) wallclockProgressionMs;
        }
        LOG.debug(
            "unbounded generator backlog now {}ms ({} bytes) at {}us interEventDelay "
                + "with {} time dilation",
            backlogDurationMs,
            backlogBytes,
            interEventDelayUs,
            timeDialation);
        lastReportedBacklogWallclock = now;
        if (pendingEvent != null) {
          timestampAtLastReportedBacklogMs = pendingEvent.getTimestamp().getMillis();
        }
      }
    }

    @Override
    public Event getCurrent() {
      if (currentEvent == null) {
        throw new NoSuchElementException();
      }
      return currentEvent.getValue();
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (currentEvent == null) {
        throw new NoSuchElementException();
      }
      return currentEvent.getTimestamp();
    }

    @Override
    public void close() {
      // Nothing to close.
    }

    @Override
    public UnboundedEventSource getCurrentSource() {
      return UnboundedEventSource.this;
    }

    @Override
    public Instant getWatermark() {
      return new Instant(watermark);
    }

    @Override
    public GeneratorCheckpoint getCheckpointMark() {
      return generator.toCheckpoint();
    }

    @Override
    public long getSplitBacklogBytes() {
      return backlogBytes == null ? BACKLOG_UNKNOWN : backlogBytes;
    }

    @Override
    public String toString() {
      return String.format(
          "EventReader(%d, %d, %d)",
          generator.getCurrentConfig().getStartEventId(),
          generator.getNextEventId(),
          generator.getCurrentConfig().getStopEventId());
    }
  }

  @Override
  public Coder<GeneratorCheckpoint> getCheckpointMarkCoder() {
    return GeneratorCheckpoint.CODER_INSTANCE;
  }

  @Override
  public List<UnboundedEventSource> split(int desiredNumSplits, PipelineOptions options) {
    LOG.trace("splitting unbounded source into {} sub-sources", numEventGenerators);
    List<UnboundedEventSource> results = new ArrayList<>();
    // Ignore desiredNumSplits and use numEventGenerators instead.
    for (GeneratorConfig subConfig : config.split(numEventGenerators)) {
      results.add(new UnboundedEventSource(subConfig, 1, watermarkHoldbackSec, isRateLimited));
    }
    return results;
  }

  @Override
  public EventReader createReader(
      PipelineOptions options, @Nullable GeneratorCheckpoint checkpoint) {
    if (checkpoint == null) {
      LOG.trace("creating initial unbounded reader for {}", config);
      return new EventReader(config);
    } else {
      LOG.trace("resuming unbounded reader from {}", checkpoint);
      return new EventReader(checkpoint.toGenerator(config));
    }
  }

  @Override
  public void validate() {
    // Nothing to validate.
  }

  @Override
  public Coder<Event> getDefaultOutputCoder() {
    return Event.CODER;
  }

  @Override
  public String toString() {
    return String.format(
        "UnboundedEventSource(%d, %d)", config.getStartEventId(), config.getStopEventId());
  }
}
