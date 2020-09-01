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
package org.apache.beam.examples.complete.game;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.examples.complete.game.utils.WriteToText;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the first in a series of four pipelines that tell a story in a 'gaming' domain.
 * Concepts: batch processing, reading input from text files, writing output to text files, using
 * standalone DoFns, use of the sum per key transform, and use of Java 8 lambda syntax.
 *
 * <p>In this gaming scenario, many users play, as members of different teams, over the course of a
 * day, and their actions are logged for processing. Some of the logged game events may be late-
 * arriving, if users play on mobile devices and go transiently offline for a period.
 *
 * <p>This pipeline does batch processing of data collected from gaming events. It calculates the
 * sum of scores per user, over an entire batch of gaming data (collected, say, for each day). The
 * batch processing will not include any late data that arrives after the day's cutoff point.
 *
 * <p>To execute this pipeline, specify the pipeline configuration like this:
 *
 * <pre>{@code
 * --tempLocation=YOUR_TEMP_DIRECTORY
 * --runner=YOUR_RUNNER
 * --output=YOUR_OUTPUT_DIRECTORY
 * (possibly options specific to your runner or permissions for your temp/output locations)
 * }</pre>
 *
 * <p>Optionally include the --input argument to specify a batch input file. See the --input default
 * value for example batch data file, or use {@code injector.Injector} to generate your own batch
 * data.
 */
public class UserScore {

  /** Class to hold info about a game event. */
  @DefaultCoder(AvroCoder.class)
  static class GameActionInfo {
    @Nullable String user;
    @Nullable String team;
    @Nullable Integer score;
    @Nullable Long timestamp;

    public GameActionInfo() {}

    public GameActionInfo(String user, String team, Integer score, Long timestamp) {
      this.user = user;
      this.team = team;
      this.score = score;
      this.timestamp = timestamp;
    }

    public String getUser() {
      return this.user;
    }

    public String getTeam() {
      return this.team;
    }

    public Integer getScore() {
      return this.score;
    }

    public Long getTimestamp() {
      return this.timestamp;
    }

    public String getKey(String keyname) {
      if ("team".equals(keyname)) {
        return this.team;
      } else { // return username as default
        return this.user;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || o.getClass() != this.getClass()) {
        return false;
      }

      GameActionInfo gameActionInfo = (GameActionInfo) o;

      if (!this.getUser().equals(gameActionInfo.getUser())) {
        return false;
      }

      if (!this.getTeam().equals(gameActionInfo.getTeam())) {
        return false;
      }

      if (!this.getScore().equals(gameActionInfo.getScore())) {
        return false;
      }

      return this.getTimestamp().equals(gameActionInfo.getTimestamp());
    }

    @Override
    public int hashCode() {
      return Objects.hash(user, team, score, timestamp);
    }
  }

  /**
   * Parses the raw game event info into GameActionInfo objects. Each event line has the following
   * format: username,teamname,score,timestamp_in_ms,readable_time e.g.:
   * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224 The human-readable
   * time string is not used here.
   */
  static class ParseEventFn extends DoFn<String, GameActionInfo> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] components = c.element().split(",", -1);
      try {
        String user = components[0].trim();
        String team = components[1].trim();
        Integer score = Integer.parseInt(components[2].trim());
        Long timestamp = Long.parseLong(components[3].trim());
        GameActionInfo gInfo = new GameActionInfo(user, team, score, timestamp);
        c.output(gInfo);
      } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        numParseErrors.inc();
        LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
      }
    }
  }

  /**
   * A transform to extract key/score information from GameActionInfo, and sum the scores. The
   * constructor arg determines whether 'team' or 'user' info is extracted.
   */
  // [START DocInclude_USExtractXform]
  public static class ExtractAndSumScore
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

    private final String field;

    ExtractAndSumScore(String field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> gameInfo) {

      return gameInfo
          .apply(
              MapElements.into(
                      TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                  .via((GameActionInfo gInfo) -> KV.of(gInfo.getKey(field), gInfo.getScore())))
          .apply(Sum.integersPerKey());
    }
  }
  // [END DocInclude_USExtractXform]

  /** Options supported by {@link UserScore}. */
  public interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing game data.")
    /* The default maps to two large Google Cloud Storage files (each ~12GB) holding two subsequent
    day's worth (roughly) of data.

    Note: You may want to use a small sample dataset to test it locally/quickly : gs://apache-beam-samples/game/small/gaming_data.csv
    You can also download it via the command line gsutil cp gs://apache-beam-samples/game/small/gaming_data.csv ./destination_folder/gaming_data.csv */
    @Default.String("gs://apache-beam-samples/game/gaming_data*.csv")
    String getInput();

    void setInput(String value);

    // Set this required option to specify where to write the output.
    @Description("Path of the file to write to.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to text. This map is
   * passed to the {@link WriteToText} constructor to write user score sums.
   */
  protected static Map<String, WriteToText.FieldFn<KV<String, Integer>>> configureOutput() {
    Map<String, WriteToText.FieldFn<KV<String, Integer>>> config = new HashMap<>();
    config.put("user", (c, w) -> c.element().getKey());
    config.put("total_score", (c, w) -> c.element().getValue());
    return config;
  }

  /** Run a batch pipeline. */
  // [START DocInclude_USMain]
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Read events from a text file and parse them.
    pipeline
        .apply(TextIO.read().from(options.getInput()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        // Extract and sum username/score pairs from the event data.
        .apply("ExtractUserScore", new ExtractAndSumScore("user"))
        .apply(
            "WriteUserScoreSums", new WriteToText<>(options.getOutput(), configureOutput(), false));

    // Run the batch pipeline.
    pipeline.run().waitUntilFinish();
  }
  // [END DocInclude_USMain]
}
