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
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.examples.complete.game.utils.WriteWindowedToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the fourth in a series of four pipelines that tell a story in a 'gaming' domain,
 * following {@link UserScore}, {@link HourlyTeamScore}, and {@link LeaderBoard}. New concepts:
 * session windows and finding session duration; use of both singleton and non-singleton side
 * inputs.
 *
 * <p>This pipeline builds on the {@link LeaderBoard} functionality, and adds some "business
 * intelligence" analysis: abuse detection and usage patterns. The pipeline derives the Mean user
 * score sum for a window, and uses that information to identify likely spammers/robots. (The robots
 * have a higher click rate than the human users). The 'robot' users are then filtered out when
 * calculating the team scores.
 *
 * <p>Additionally, user sessions are tracked: that is, we find bursts of user activity using
 * session windows. Then, the mean session duration information is recorded in the context of
 * subsequent fixed windowing. (This could be used to tell us what games are giving us greater user
 * retention).
 *
 * <p>Run {@code org.apache.beam.examples.complete.game.injector.Injector} to generate pubsub data
 * for this pipeline. The {@code Injector} documentation provides more detail.
 *
 * <p>To execute this pipeline, specify the pipeline configuration like this:
 *
 * <pre>{@code
 * --project=YOUR_PROJECT_ID
 * --tempLocation=gs://YOUR_TEMP_DIRECTORY
 * --runner=YOUR_RUNNER
 * --dataset=YOUR-DATASET
 * --topic=projects/YOUR-PROJECT/topics/YOUR-TOPIC
 * }</pre>
 *
 * <p>The BigQuery dataset you specify must already exist. The PubSub topic you specify should be
 * the same topic to which the Injector is publishing.
 */
public class GameStats extends LeaderBoard {

  /**
   * Filter out all users but those with a high clickrate, which we will consider as 'spammy' users.
   * We do this by finding the mean total score per user, then using that information as a side
   * input to filter out all but those user scores that are larger than {@code (mean *
   * SCORE_WEIGHT)}.
   */
  // [START DocInclude_AbuseDetect]
  public static class CalculateSpammyUsers
      extends PTransform<PCollection<KV<String, Integer>>, PCollection<KV<String, Integer>>> {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateSpammyUsers.class);
    private static final double SCORE_WEIGHT = 2.5;

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<KV<String, Integer>> userScores) {

      // Get the sum of scores for each user.
      PCollection<KV<String, Integer>> sumScores =
          userScores.apply("UserSum", Sum.integersPerKey());

      // Extract the score from each element, and use it to find the global mean.
      final PCollectionView<Double> globalMeanScore =
          sumScores.apply(Values.create()).apply(Mean.<Integer>globally().asSingletonView());

      // Filter the user sums using the global mean.
      PCollection<KV<String, Integer>> filtered =
          sumScores.apply(
              "ProcessAndFilter",
              ParDo
                  // use the derived mean total score as a side input
                  .of(
                      new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                        private final Counter numSpammerUsers =
                            Metrics.counter("main", "SpammerUsers");

                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          Integer score = c.element().getValue();
                          Double gmc = c.sideInput(globalMeanScore);
                          if (score > (gmc * SCORE_WEIGHT)) {
                            LOG.info(
                                "user "
                                    + c.element().getKey()
                                    + " spammer score "
                                    + score
                                    + " with mean "
                                    + gmc);
                            numSpammerUsers.inc();
                            c.output(c.element());
                          }
                        }
                      })
                  .withSideInputs(globalMeanScore));
      return filtered;
    }
  }
  // [END DocInclude_AbuseDetect]

  /** Calculate and output an element's session duration. */
  private static class UserSessionInfoFn extends DoFn<KV<String, Integer>, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      IntervalWindow w = (IntervalWindow) window;
      int duration = new Duration(w.start(), w.end()).toPeriod().toStandardMinutes().getMinutes();
      c.output(duration);
    }
  }

  /** Options supported by {@link GameStats}. */
  public interface Options extends LeaderBoard.Options {
    @Description("Numeric value of fixed window duration for user analysis, in minutes")
    @Default.Integer(60)
    Integer getFixedWindowDuration();

    void setFixedWindowDuration(Integer value);

    @Description("Numeric value of gap between user sessions, in minutes")
    @Default.Integer(5)
    Integer getSessionGap();

    void setSessionGap(Integer value);

    @Description(
        "Numeric value of fixed window for finding mean of user session duration, " + "in minutes")
    @Default.Integer(30)
    Integer getUserActivityWindowDuration();

    void setUserActivityWindowDuration(Integer value);

    @Description("Prefix used for the BigQuery table names")
    @Default.String("game_stats")
    String getGameStatsTablePrefix();

    void setGameStatsTablePrefix(String value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write information about team score sums.
   */
  protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>
      configureWindowedWrite() {
    Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<>();
    tableConfigure.put(
        "team", new WriteWindowedToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteWindowedToBigQuery.FieldInfo<>("INTEGER", (c, w) -> c.element().getValue()));
    tableConfigure.put(
        "window_start",
        new WriteWindowedToBigQuery.FieldInfo<>(
            "STRING",
            (c, w) -> {
              IntervalWindow window = (IntervalWindow) w;
              return GameConstants.DATE_TIME_FORMATTER.print(window.start());
            }));
    tableConfigure.put(
        "processing_time",
        new WriteWindowedToBigQuery.FieldInfo<>(
            "STRING", (c, w) -> GameConstants.DATE_TIME_FORMATTER.print(Instant.now())));
    return tableConfigure;
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write information about mean user session time.
   */
  protected static Map<String, WriteWindowedToBigQuery.FieldInfo<Double>>
      configureSessionWindowWrite() {

    Map<String, WriteWindowedToBigQuery.FieldInfo<Double>> tableConfigure = new HashMap<>();
    tableConfigure.put(
        "window_start",
        new WriteWindowedToBigQuery.FieldInfo<>(
            "STRING",
            (c, w) -> {
              IntervalWindow window = (IntervalWindow) w;
              return GameConstants.DATE_TIME_FORMATTER.print(window.start());
            }));
    tableConfigure.put(
        "mean_duration", new WriteWindowedToBigQuery.FieldInfo<>("FLOAT", (c, w) -> c.element()));
    return tableConfigure;
  }

  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    ExampleUtils exampleUtils = new ExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    // Read Events from Pub/Sub using custom timestamps
    PCollection<GameActionInfo> rawEvents =
        pipeline
            .apply(
                PubsubIO.readStrings()
                    .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
                    .fromTopic(options.getTopic()))
            .apply("ParseGameEvent", ParDo.of(new ParseEventFn()));

    // Extract username/score pairs from the event stream
    PCollection<KV<String, Integer>> userEvents =
        rawEvents.apply(
            "ExtractUserScore",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                .via((GameActionInfo gInfo) -> KV.of(gInfo.getUser(), gInfo.getScore())));

    // Calculate the total score per user over fixed windows, and
    // cumulative updates for late data.
    final PCollectionView<Map<String, Integer>> spammersView =
        userEvents
            .apply(
                "FixedWindowsUser",
                Window.into(
                    FixedWindows.of(Duration.standardMinutes(options.getFixedWindowDuration()))))

            // Filter out everyone but those with (SCORE_WEIGHT * avg) clickrate.
            // These might be robots/spammers.
            .apply("CalculateSpammyUsers", new CalculateSpammyUsers())
            // Derive a view from the collection of spammer users. It will be used as a side input
            // in calculating the team score sums, below.
            .apply("CreateSpammersView", View.asMap());

    // [START DocInclude_FilterAndCalc]
    // Calculate the total score per team over fixed windows,
    // and emit cumulative updates for late data. Uses the side input derived above-- the set of
    // suspected robots-- to filter out scores from those users from the sum.
    // Write the results to BigQuery.
    rawEvents
        .apply(
            "WindowIntoFixedWindows",
            Window.into(
                FixedWindows.of(Duration.standardMinutes(options.getFixedWindowDuration()))))
        // Filter out the detected spammer users, using the side input derived above.
        .apply(
            "FilterOutSpammers",
            ParDo.of(
                    new DoFn<GameActionInfo, GameActionInfo>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        // If the user is not in the spammers Map, output the data element.
                        if (c.sideInput(spammersView).get(c.element().getUser().trim()) == null) {
                          c.output(c.element());
                        }
                      }
                    })
                .withSideInputs(spammersView))
        // Extract and sum teamname/score pairs from the event data.
        .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
        // [END DocInclude_FilterAndCalc]
        // Write the result to BigQuery
        .apply(
            "WriteTeamSums",
            new WriteWindowedToBigQuery<>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getGameStatsTablePrefix() + "_team",
                configureWindowedWrite()));

    // [START DocInclude_SessionCalc]
    // Detect user sessions-- that is, a burst of activity separated by a gap from further
    // activity. Find and record the mean session lengths.
    // This information could help the game designers track the changing user engagement
    // as their set of games changes.
    userEvents
        .apply(
            "WindowIntoSessions",
            Window.<KV<String, Integer>>into(
                    Sessions.withGapDuration(Duration.standardMinutes(options.getSessionGap())))
                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW))
        // For this use, we care only about the existence of the session, not any particular
        // information aggregated over it, so the following is an efficient way to do that.
        .apply(Combine.perKey(x -> 0))
        // Get the duration per session.
        .apply("UserSessionActivity", ParDo.of(new UserSessionInfoFn()))
        // [END DocInclude_SessionCalc]
        // [START DocInclude_Rewindow]
        // Re-window to process groups of session sums according to when the sessions complete.
        .apply(
            "WindowToExtractSessionMean",
            Window.into(
                FixedWindows.of(Duration.standardMinutes(options.getUserActivityWindowDuration()))))
        // Find the mean session duration in each window.
        .apply(Mean.<Integer>globally().withoutDefaults())
        // Write this info to a BigQuery table.
        .apply(
            "WriteAvgSessionLength",
            new WriteWindowedToBigQuery<>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getGameStatsTablePrefix() + "_sessions",
                configureSessionWindowWrite()));
    // [END DocInclude_Rewindow]

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
    exampleUtils.waitToFinish(result);
  }
}
