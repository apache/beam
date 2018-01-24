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
package org.apache.beam.examples.complete;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.CalendarWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An example that reads Wikipedia edit data from Cloud Storage and computes the user with
 * the longest string of edits separated by no more than an hour within each month.
 *
 * <p>Concepts: Using Windowing to perform time-based aggregations of data.
 *
 * <p>It is not recommended to execute this pipeline locally, given the size of the default input
 * data.
 *
 * <p>To execute this pipeline using a selected runner and an output prefix on GCS, specify:
 * <pre>{@code
 *   --runner=YOUR_SELECTED_RUNNER
 *   --output=gs://YOUR_OUTPUT_PREFIX
 * }
 * </pre>
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 * <p>The default input is {@code gs://apache-beam-samples/wikipedia_edits/*.json} and can be
 * overridden with {@code --input}.
 */
public class TopWikipediaSessions {
  private static final String EXPORTED_WIKI_TABLE =
      "gs://apache-beam-samples/wikipedia_edits/*.json";

  /**
   * Extracts user and timestamp from a TableRow representing a Wikipedia edit.
   */
  static class ExtractUserAndTimestamp extends DoFn<TableRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      int timestamp = (Integer) row.get("timestamp");
      String userName = (String) row.get("contributor_username");
      if (userName != null) {
        // Sets the implicit timestamp field to be used in windowing.
        c.outputWithTimestamp(userName, new Instant(timestamp * 1000L));
      }
    }
  }

  /**
   * Computes the number of edits in each user session.  A session is defined as
   * a string of edits where each is separated from the next by less than an hour.
   */
  static class ComputeSessions
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> actions) {
      return actions
          .apply(Window.into(Sessions.withGapDuration(Duration.standardHours(1))))
          .apply(Count.perElement());
    }
  }

  /**
   * Computes the longest session ending in each month.
   */
  private static class TopPerMonth
      extends PTransform<PCollection<KV<String, Long>>, PCollection<List<KV<String, Long>>>> {
    @Override
    public PCollection<List<KV<String, Long>>> expand(PCollection<KV<String, Long>> sessions) {
      SerializableComparator<KV<String, Long>> comparator =
          (o1, o2) -> Long.compare(o1.getValue(), o2.getValue());
      return sessions
          .apply(Window.into(CalendarWindows.months(1)))
          .apply(Top.of(1, comparator).withoutDefaults());
    }
  }

  static class SessionsToStringsDoFn extends DoFn<KV<String, Long>, KV<String, Long>> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      c.output(KV.of(
          c.element().getKey() + " : " + window, c.element().getValue()));
    }
  }

  static class FormatOutputDoFn extends DoFn<List<KV<String, Long>>, String> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      for (KV<String, Long> item : c.element()) {
        String session = item.getKey();
        long count = item.getValue();
        c.output(session + " : " + count + " : " + ((IntervalWindow) window).start());
      }
    }
  }

  static class ParseTableRowJson extends SimpleFunction<String, TableRow> {
    @Override
    public TableRow apply(String input) {
      try {
        return Transport.getJsonFactory().fromString(input, TableRow.class);
      } catch (IOException e) {
        throw new RuntimeException("Failed parsing table row json", e);
      }
    }
  }

  static class ComputeTopSessions extends PTransform<PCollection<TableRow>, PCollection<String>> {

    private final double samplingThreshold;

    public ComputeTopSessions(double samplingThreshold) {
      this.samplingThreshold = samplingThreshold;
    }

    @Override
    public PCollection<String> expand(PCollection<TableRow> input) {
      return input
          .apply(ParDo.of(new ExtractUserAndTimestamp()))
          .apply(
              "SampleUsers",
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      if (Math.abs((long) c.element().hashCode())
                          <= Integer.MAX_VALUE * samplingThreshold) {
                        c.output(c.element());
                      }
                    }
                  }))
          .apply(new ComputeSessions())
          .apply("SessionsToStrings", ParDo.of(new SessionsToStringsDoFn()))
          .apply(new TopPerMonth())
          .apply("FormatOutput", ParDo.of(new FormatOutputDoFn()));
    }
  }

  /**
   * Options supported by this class.
   *
   * <p>Inherits standard Beam configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description(
      "Input specified as a GCS path containing a BigQuery table exported as json")
    @Default.String(EXPORTED_WIKI_TABLE)
    String getInput();
    void setInput(String value);
    @Description("File to output results to")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    Pipeline p = Pipeline.create(options);

    double samplingThreshold = 0.1;

    p.apply(TextIO.read().from(options.getInput()))
        .apply(MapElements.via(new ParseTableRowJson()))
        .apply(new ComputeTopSessions(samplingThreshold))
        .apply("Write", TextIO.write().withoutSharding().to(options.getOutput()));

    p.run().waitUntilFinish();
  }
}
