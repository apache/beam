/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.integration.nexmark;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.Keys;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum.SumLongFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterEach;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterFirst;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.GcsIOChannelFactory;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import com.google.common.base.Preconditions;

import org.joda.time.Duration;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;

/**
 * Query "10", 'Log to sharded files' (Not in original suite.)
 * <p>
 * <p>Every windowSizeSec, save all events from the last our into 2*maxWorkers log files.
 */
class Query10 extends NexmarkQuery {
  private static final int CHANNEL_BUFFER = 8 << 20; // 8MB
  private static final int NUM_SHARDS_PER_WORKER = 5;
  private static final Duration TEN_SECONDS = Duration.standardSeconds(10);

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

  private WritableByteChannel openGcsFile(GcsOptions options, String filename) throws IOException {
    WritableByteChannel channel = new GcsIOChannelFactory(options).create(filename, "text/plain");
    Preconditions.checkState(channel instanceof GoogleCloudStorageWriteChannel);
    ((GoogleCloudStorageWriteChannel) channel).setUploadBufferSize(CHANNEL_BUFFER);
    return channel;
  }

  @Nullable
  private String buildOutputPath(
      String key, BoundedWindow window, PaneInfo.Timing timing, long paneIndex) {
    if (outputPath == null) {
      return null;
    }
    String which = null;
    switch (timing) {
      case EARLY:
        which = "E";
        break;
      case ON_TIME:
        which = "O";
        break;
      case LATE:
        which = "L";
        break;
    }
    return String.format("%s/%s-%s-%03d%s", outputPath, window.maxTimestamp(), key,
                         paneIndex, which);
  }

  @Nullable
  private String buildFinalizeOutputPath(BoundedWindow window) {
    if (outputPath == null) {
      return null;
    }
    return String.format("%s/_SUCCESS-%s", outputPath, window.maxTimestamp());
  }

  private PCollection<Done> applyTyped(PCollection<Event> events) {
    final int numLogShards = maxNumWorkers * NUM_SHARDS_PER_WORKER;

    return events
        .apply(ParDo.named(name + ".ShardEvents")
                    .of(new DoFn<Event, KV<String, Event>>() {
                      final Aggregator<Long, Long> lateCounter =
                          createAggregator("actuallyLateEvent", new SumLongFn());
                      final Aggregator<Long, Long> onTimeCounter =
                          createAggregator("actuallyOnTimeEvent", new SumLongFn());

                      @Override
                      public void processElement(ProcessContext c) {
                        if (c.element().hasAnnotation("LATE")) {
                          lateCounter.addValue(1L);
                          NexmarkUtils.error("Observed late: %s", c.element());
                        } else {
                          onTimeCounter.addValue(1L);
                        }
                        int shard = (int) Math.abs((long) c.element().hashCode() % numLogShards);
                        c.output(KV.of("dummy-" + shard, c.element()));
                      }
                    }))
        .apply(Window.<KV<String, Event>>into(
            FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec)))
                   .named(name + ".WindowEvents")
                   .triggering(AfterEach.inOrder(
                       Repeatedly
                           .forever(AfterPane.elementCountAtLeast(configuration.maxLogEvents))
                           .orFinally(AfterWatermark.pastEndOfWindow()),
                       Repeatedly.forever(
                           AfterFirst.of(AfterPane.elementCountAtLeast(configuration.maxLogEvents),
                                         AfterProcessingTime.pastFirstElementInPane().plusDelayOf
                                             (TEN_SECONDS)))))
                   .discardingFiredPanes()
                   // Use a 1 day allowed lateness so that any forgotten hold will stall the
                   // pipeline for that period and be very noticable.
                   .withAllowedLateness(Duration.standardDays(1)))
        .apply(GroupByKey.<String, Event>create())
        .apply(
            ParDo.named(name + ".CheckForLateEvents")
                 .of(new DoFnWithContext<KV<String, Iterable<Event>>,
                     KV<String, Iterable<Event>>>() {
                   final Aggregator<Long, Long> earlyCounter =
                       createAggregator("earlyShard", new SumLongFn());
                   final Aggregator<Long, Long> onTimeCounter =
                       createAggregator("onTimeShard", new SumLongFn());
                   final Aggregator<Long, Long> lateCounter =
                       createAggregator("lateShard", new SumLongFn());
                   final Aggregator<Long, Long> unexpectedLatePaneCounter =
                       createAggregator("ERROR_unexpectedLatePane", new SumLongFn());
                   final Aggregator<Long, Long> unexpectedOnTimeElementCounter =
                       createAggregator("ERROR_unexpectedOnTimeElement", new SumLongFn());

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
                     String key = c.element().getKey();
                     NexmarkUtils.error(
                         "key %s with timestamp %s has %d actually late and %d on-time "
                         + "elements in pane %s for window %s",
                         key, c.timestamp(), numLate, numOnTime, c.pane(),
                         window.maxTimestamp());
                     if (c.pane().getTiming() == PaneInfo.Timing.LATE) {
                       if (numLate == 0) {
                         NexmarkUtils.error(
                             "ERROR! No late events in late pane for key %s", key);
                         unexpectedLatePaneCounter.addValue(1L);
                       }
                       if (numOnTime > 0) {
                         NexmarkUtils.error(
                             "ERROR! Have %d on-time events in late pane for key %s",
                             numOnTime, key);
                         unexpectedOnTimeElementCounter.addValue(1L);
                       }
                       lateCounter.addValue(1L);
                     } else if (c.pane().getTiming() == PaneInfo.Timing.EARLY) {
                       if (numOnTime + numLate < configuration.maxLogEvents) {
                         NexmarkUtils.error(
                             "ERROR! Only have %d events in early pane for key %s",
                             numOnTime + numLate, key);
                       }
                       earlyCounter.addValue(1L);
                     } else {
                       onTimeCounter.addValue(1L);
                     }
                     c.output(c.element());
                   }
                 }))
        .apply(
            ParDo.named(name + ".UploadEvents")
                 .of(new DoFnWithContext<KV<String, Iterable<Event>>, KV<Void, Void>>() {
                   final Aggregator<Long, Long> savedFileCounter =
                       createAggregator("savedFile", new SumLongFn());
                   final Aggregator<Long, Long> writtenRecordsCounter =
                       createAggregator("writtenRecords", new SumLongFn());

                   @ProcessElement
                   public void process(ProcessContext c, BoundedWindow window) throws IOException {
                     String key = c.element().getKey();
                     GcsOptions options = c.getPipelineOptions().as(GcsOptions.class);
                     NexmarkUtils.error(
                         "Writing key %s with record timestamp %s, window timestamp %s, pane %s",
                         key, c.timestamp(), window.maxTimestamp(), c.pane());
                     String path = buildOutputPath(key, window, c.pane().getTiming(), c.pane()
                                                                                       .getIndex());
                     if (path != null) {
                       NexmarkUtils.error("Beginning write for '%s'", path);
                       int n = 0;
                       try (OutputStream output = Channels.newOutputStream(openGcsFile(options,
                                                                                       path))) {
                         for (Event event : c.element().getValue()) {
                           Event.CODER.encode(event, output, Coder.Context.OUTER);
                           writtenRecordsCounter.addValue(1L);
                           if (++n % 10000 == 0) {
                             NexmarkUtils.error("So far written %d records to '%s'", n, path);
                           }
                         }
                       }
                       NexmarkUtils.error("Written all %d records to '%s'", n, path);
                     }
                     savedFileCounter.addValue(1L);
                     c.output(KV.<Void, Void>of(null, null));
                   }
                 }))
        // Clear fancy triggering from above.
        .apply(Window.<KV<Void, Void>>into(
            FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec)))
                   .named(name + ".WindowLogFiles")
                   .triggering(AfterWatermark.pastEndOfWindow())
                   // We expect no late data here, but we'll assume the worst so we can detect any.
                   .withAllowedLateness(Duration.standardDays(1))
                   .discardingFiredPanes())
        .apply(GroupByKey.<Void, Void>create())
        .apply(Keys.<Void>create())
        .apply(
            ParDo.named(name + ".Finalize")
                 .of(new DoFnWithContext<Void, Done>() {
                   final Aggregator<Long, Long> unexpectedLateCounter =
                       createAggregator("ERROR_unexpectedLate", new SumLongFn());
                   final Aggregator<Long, Long> unexpectedEarlyCounter =
                       createAggregator("ERROR_unexpectedEarly", new SumLongFn());
                   final Aggregator<Long, Long> unexpectedIndexCounter =
                       createAggregator("ERROR_unexpectedIndex", new SumLongFn());
                   final Aggregator<Long, Long> finalizedCounter =
                       createAggregator("finalized", new SumLongFn());

                   @ProcessElement
                   public void process(ProcessContext c, BoundedWindow window) throws IOException {
                     if (c.pane().getTiming() == Timing.LATE) {
                       unexpectedLateCounter.addValue(1L);
                       NexmarkUtils.error("ERROR! Unexpected LATE pane: %s", c.pane());
                     } else if (c.pane().getTiming() == Timing.EARLY) {
                       unexpectedEarlyCounter.addValue(1L);
                       NexmarkUtils.error("ERROR! Unexpected EARLY pane: %s", c.pane());
                     } else if (c.pane().getTiming() == Timing.ON_TIME
                                && c.pane().getIndex() != 0) {
                       unexpectedIndexCounter.addValue(1L);
                       NexmarkUtils.error("ERROR! Unexpected ON_TIME pane index: %s", c.pane());
                     } else {
                       GcsOptions options = c.getPipelineOptions().as(GcsOptions.class);
                       NexmarkUtils.error(
                           "Finalize with record timestamp %s, window timestamp %s, pane %s",
                           c.timestamp(), window.maxTimestamp(), c.pane());
                       String path = buildFinalizeOutputPath(window);
                       if (path != null) {
                         NexmarkUtils.error("Beginning write for '%s'", path);
                         try (WritableByteChannel output = openGcsFile(options, path)) {
                           output.write(ByteBuffer.wrap("FINISHED".getBytes()));
                         }
                         NexmarkUtils.error("Written '%s'", path);
                       }
                       c.output(
                           new Done("written for timestamp " + window.maxTimestamp().getMillis()));
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
