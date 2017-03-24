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
package org.apache.beam.integration.nexmark.queries;

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.integration.nexmark.NexmarkConfiguration;
import org.apache.beam.integration.nexmark.NexmarkQuery;
import org.apache.beam.integration.nexmark.NexmarkUtils;
import org.apache.beam.integration.nexmark.model.Done;
import org.apache.beam.integration.nexmark.model.Event;
import org.apache.beam.integration.nexmark.model.KnownSize;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.GcsIOChannelFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Query "10", 'Log to sharded files' (Not in original suite.)
 *
 * <p>Every windowSizeSec, save all events from the last period into 2*maxWorkers log files.
 */
public class Query10 extends NexmarkQuery {
  private static final Logger LOG = LoggerFactory.getLogger(Query10.class);
  private static final int CHANNEL_BUFFER = 8 << 20; // 8MB
  private static final int NUM_SHARDS_PER_WORKER = 5;
  private static final Duration LATE_BATCHING_PERIOD = Duration.standardSeconds(10);

  /**
   * Capture everything we need to know about the records in a single output file.
   */
  private static class OutputFile implements Serializable {
    /** Maximum possible timestamp of records in file. */
    private final Instant maxTimestamp;
    /** Shard within window. */
    private final String shard;
    /** Index of file in all files in shard. */
    private final long index;
    /** Timing of records in this file. */
    private final PaneInfo.Timing timing;
    /** Path to file containing records, or {@literal null} if no output required. */
    @Nullable
    private final String filename;

    public OutputFile(
        Instant maxTimestamp,
        String shard,
        long index,
        PaneInfo.Timing timing,
        @Nullable String filename) {
      this.maxTimestamp = maxTimestamp;
      this.shard = shard;
      this.index = index;
      this.timing = timing;
      this.filename = filename;
    }

    @Override
    public String toString() {
      return String.format("%s %s %d %s %s\n", maxTimestamp, shard, index, timing, filename);
    }
  }

  /**
   * GCS uri prefix for all log and 'finished' files. If null they won't be written.
   */
  @Nullable
  private String outputPath;

  /**
   * Maximum number of workers, used to determine log sharding factor.
   */
  private int maxNumWorkers;

  public Query10(NexmarkConfiguration configuration) {
    super(configuration, "Query10");
  }

  public void setOutputPath(@Nullable String outputPath) {
    this.outputPath = outputPath;
  }

  public void setMaxNumWorkers(int maxNumWorkers) {
    this.maxNumWorkers = maxNumWorkers;
  }

  /**
   * Return channel for writing bytes to GCS.
   *
   * @throws IOException
   */
  private WritableByteChannel openWritableGcsFile(GcsOptions options, String filename)
      throws IOException {
    WritableByteChannel channel =
            GcsIOChannelFactory.fromOptions(options).create(filename, "text/plain");
    checkState(channel instanceof GoogleCloudStorageWriteChannel);
    ((GoogleCloudStorageWriteChannel) channel).setUploadBufferSize(CHANNEL_BUFFER);
    return channel;
  }

  /** Return a short string to describe {@code timing}. */
  private String timingToString(PaneInfo.Timing timing) {
    switch (timing) {
      case EARLY:
        return "E";
      case ON_TIME:
        return "O";
      case LATE:
        return "L";
    }
    throw new RuntimeException(); // cases are exhaustive
  }

  /** Construct an {@link OutputFile} for {@code pane} in {@code window} for {@code shard}. */
  private OutputFile outputFileFor(BoundedWindow window, String shard, PaneInfo pane) {
    @Nullable String filename =
        outputPath == null
        ? null
        : String.format("%s/LOG-%s-%s-%03d-%s-%x",
            outputPath, window.maxTimestamp(), shard, pane.getIndex(),
            timingToString(pane.getTiming()),
            ThreadLocalRandom.current().nextLong());
    return new OutputFile(window.maxTimestamp(), shard, pane.getIndex(),
        pane.getTiming(), filename);
  }

  /**
   * Return path to which we should write the index for {@code window}, or {@literal null}
   * if no output required.
   */
  @Nullable
  private String indexPathFor(BoundedWindow window) {
    if (outputPath == null) {
      return null;
    }
    return String.format("%s/INDEX-%s", outputPath, window.maxTimestamp());
  }

  private PCollection<Done> applyTyped(PCollection<Event> events) {
    final int numLogShards = maxNumWorkers * NUM_SHARDS_PER_WORKER;

    return events.apply(name + ".ShardEvents",
            ParDo.of(new DoFn<Event, KV<String, Event>>() {
                      final Aggregator<Long, Long> lateCounter =
                          createAggregator("actuallyLateEvent", Sum.ofLongs());
                      final Aggregator<Long, Long> onTimeCounter =
                          createAggregator("actuallyOnTimeEvent", Sum.ofLongs());

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        if (c.element().hasAnnotation("LATE")) {
                          lateCounter.addValue(1L);
                          LOG.error("Observed late: %s", c.element());
                        } else {
                          onTimeCounter.addValue(1L);
                        }
                        int shardNum = (int) Math.abs((long) c.element().hashCode() % numLogShards);
                        String shard = String.format("shard-%05d-of-%05d", shardNum, numLogShards);
                        c.output(KV.of(shard, c.element()));
                      }
                    }))
        .apply(name + ".WindowEvents",
                Window.<KV<String, Event>>into(
            FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec)))
            .triggering(AfterEach.inOrder(
                Repeatedly
                    .forever(AfterPane.elementCountAtLeast(configuration.maxLogEvents))
                    .orFinally(AfterWatermark.pastEndOfWindow()),
                Repeatedly.forever(
                    AfterFirst.of(AfterPane.elementCountAtLeast(configuration.maxLogEvents),
                        AfterProcessingTime.pastFirstElementInPane()
                                           .plusDelayOf(LATE_BATCHING_PERIOD)))))
            .discardingFiredPanes()
            // Use a 1 day allowed lateness so that any forgotten hold will stall the
            // pipeline for that period and be very noticeable.
            .withAllowedLateness(Duration.standardDays(1)))
        .apply(name + ".GroupByKey", GroupByKey.<String, Event>create())
        .apply(name + ".CheckForLateEvents",
            ParDo.of(new DoFn<KV<String, Iterable<Event>>,
                     KV<String, Iterable<Event>>>() {
                   final Aggregator<Long, Long> earlyCounter =
                       createAggregator("earlyShard", Sum.ofLongs());
                   final Aggregator<Long, Long> onTimeCounter =
                       createAggregator("onTimeShard", Sum.ofLongs());
                   final Aggregator<Long, Long> lateCounter =
                       createAggregator("lateShard", Sum.ofLongs());
                   final Aggregator<Long, Long> unexpectedLatePaneCounter =
                       createAggregator("ERROR_unexpectedLatePane", Sum.ofLongs());
                   final Aggregator<Long, Long> unexpectedOnTimeElementCounter =
                       createAggregator("ERROR_unexpectedOnTimeElement", Sum.ofLongs());

                   @ProcessElement
                   public void processElement(ProcessContext c, BoundedWindow window) {
                     int numLate = 0;
                     int numOnTime = 0;
                     for (Event event : c.element().getValue()) {
                       if (event.hasAnnotation("LATE")) {
                         numLate++;
                       } else {
                         numOnTime++;
                       }
                     }
                     String shard = c.element().getKey();
                     LOG.error(
                         "%s with timestamp %s has %d actually late and %d on-time "
                             + "elements in pane %s for window %s",
                         shard, c.timestamp(), numLate, numOnTime, c.pane(),
                         window.maxTimestamp());
                     if (c.pane().getTiming() == PaneInfo.Timing.LATE) {
                       if (numLate == 0) {
                         LOG.error(
                             "ERROR! No late events in late pane for %s", shard);
                         unexpectedLatePaneCounter.addValue(1L);
                       }
                       if (numOnTime > 0) {
                         LOG.error(
                             "ERROR! Have %d on-time events in late pane for %s",
                             numOnTime, shard);
                         unexpectedOnTimeElementCounter.addValue(1L);
                       }
                       lateCounter.addValue(1L);
                     } else if (c.pane().getTiming() == PaneInfo.Timing.EARLY) {
                       if (numOnTime + numLate < configuration.maxLogEvents) {
                         LOG.error(
                             "ERROR! Only have %d events in early pane for %s",
                             numOnTime + numLate, shard);
                       }
                       earlyCounter.addValue(1L);
                     } else {
                       onTimeCounter.addValue(1L);
                     }
                     c.output(c.element());
                   }
                 }))
        .apply(name + ".UploadEvents",
            ParDo.of(new DoFn<KV<String, Iterable<Event>>,
                     KV<Void, OutputFile>>() {
                   final Aggregator<Long, Long> savedFileCounter =
                       createAggregator("savedFile", Sum.ofLongs());
                   final Aggregator<Long, Long> writtenRecordsCounter =
                       createAggregator("writtenRecords", Sum.ofLongs());

                   @ProcessElement
                   public void processElement(ProcessContext c, BoundedWindow window)
                           throws IOException {
                     String shard = c.element().getKey();
                     GcsOptions options = c.getPipelineOptions().as(GcsOptions.class);
                     OutputFile outputFile = outputFileFor(window, shard, c.pane());
                     LOG.error(
                         "Writing %s with record timestamp %s, window timestamp %s, pane %s",
                         shard, c.timestamp(), window.maxTimestamp(), c.pane());
                     if (outputFile.filename != null) {
                       LOG.error("Beginning write to '%s'", outputFile.filename);
                       int n = 0;
                       try (OutputStream output =
                                Channels.newOutputStream(openWritableGcsFile(options, outputFile
                                    .filename))) {
                         for (Event event : c.element().getValue()) {
                           Event.CODER.encode(event, output, Coder.Context.OUTER);
                           writtenRecordsCounter.addValue(1L);
                           if (++n % 10000 == 0) {
                             LOG.error("So far written %d records to '%s'", n,
                                 outputFile.filename);
                           }
                         }
                       }
                       LOG.error("Written all %d records to '%s'", n, outputFile.filename);
                     }
                     savedFileCounter.addValue(1L);
                     c.output(KV.<Void, OutputFile>of(null, outputFile));
                   }
                 }))
        // Clear fancy triggering from above.
        .apply(name + ".WindowLogFiles", Window.<KV<Void, OutputFile>>into(
            FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec)))
            .triggering(AfterWatermark.pastEndOfWindow())
            // We expect no late data here, but we'll assume the worst so we can detect any.
            .withAllowedLateness(Duration.standardDays(1))
            .discardingFiredPanes())
      // this GroupByKey allows to have one file per window
      .apply(name + ".GroupByKey2", GroupByKey.<Void, OutputFile>create())
        .apply(name + ".Index",
            ParDo.of(new DoFn<KV<Void, Iterable<OutputFile>>, Done>() {
                   final Aggregator<Long, Long> unexpectedLateCounter =
                       createAggregator("ERROR_unexpectedLate", Sum.ofLongs());
                   final Aggregator<Long, Long> unexpectedEarlyCounter =
                       createAggregator("ERROR_unexpectedEarly", Sum.ofLongs());
                   final Aggregator<Long, Long> unexpectedIndexCounter =
                       createAggregator("ERROR_unexpectedIndex", Sum.ofLongs());
                   final Aggregator<Long, Long> finalizedCounter =
                       createAggregator("indexed", Sum.ofLongs());

                   @ProcessElement
                   public void processElement(ProcessContext c, BoundedWindow window)
                           throws IOException {
                     if (c.pane().getTiming() == Timing.LATE) {
                       unexpectedLateCounter.addValue(1L);
                       LOG.error("ERROR! Unexpected LATE pane: %s", c.pane());
                     } else if (c.pane().getTiming() == Timing.EARLY) {
                       unexpectedEarlyCounter.addValue(1L);
                       LOG.error("ERROR! Unexpected EARLY pane: %s", c.pane());
                     } else if (c.pane().getTiming() == Timing.ON_TIME
                         && c.pane().getIndex() != 0) {
                       unexpectedIndexCounter.addValue(1L);
                       LOG.error("ERROR! Unexpected ON_TIME pane index: %s", c.pane());
                     } else {
                       GcsOptions options = c.getPipelineOptions().as(GcsOptions.class);
                       LOG.error(
                           "Index with record timestamp %s, window timestamp %s, pane %s",
                           c.timestamp(), window.maxTimestamp(), c.pane());

                       @Nullable String filename = indexPathFor(window);
                       if (filename != null) {
                         LOG.error("Beginning write to '%s'", filename);
                         int n = 0;
                         try (OutputStream output =
                                  Channels.newOutputStream(
                                      openWritableGcsFile(options, filename))) {
                           for (OutputFile outputFile : c.element().getValue()) {
                             output.write(outputFile.toString().getBytes());
                             n++;
                           }
                         }
                         LOG.error("Written all %d lines to '%s'", n, filename);
                       }
                       c.output(
                           new Done("written for timestamp " + window.maxTimestamp()));
                       finalizedCounter.addValue(1L);
                     }
                   }
                 }));
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    return NexmarkUtils.castToKnownSize(name, applyTyped(events));
  }
}
