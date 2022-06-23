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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.examples.complete.game.utils.WriteToBigQuery.FieldInfo;
import org.apache.beam.examples.complete.game.utils.WriteWindowedToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * This class is part of a series of pipelines that tell a story in a gaming domain. Concepts
 * include: stateful processing.
 *
 * <p>This pipeline processes an unbounded stream of 'game events'. It uses stateful processing to
 * aggregate team scores per team and outputs team name and it's total score every time the team
 * passes a new multiple of a threshold score. For example, multiples of the threshold could be the
 * corresponding scores required to pass each level of the game. By default, this threshold is set
 * to 5000.
 *
 * <p>Stateful processing allows us to write pipelines that output based on a runtime state (when a
 * team reaches a certain score, in every 100 game events etc) without time triggers. See
 * https://beam.apache.org/blog/2017/02/13/stateful-processing.html for more information on using
 * stateful processing.
 *
 * <p>Run {@code injector.Injector} to generate pubsub data for this pipeline. The Injector
 * documentation provides more detail on how to do this.
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
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class StatefulTeamScore extends LeaderBoard {

  /** Options supported by {@link StatefulTeamScore}. */
  public interface Options extends LeaderBoard.Options {

    @Description("Numeric value, multiple of which is used as threshold for outputting team score.")
    @Default.Integer(5000)
    Integer getThresholdScore();

    void setThresholdScore(Integer value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write team score sums.
   */
  private static Map<String, FieldInfo<KV<String, Integer>>> configureCompleteWindowedTableWrite() {

    Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<>();
    tableConfigure.put(
        "team", new WriteWindowedToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteWindowedToBigQuery.FieldInfo<>("INTEGER", (c, w) -> c.element().getValue()));
    tableConfigure.put(
        "processing_time",
        new WriteWindowedToBigQuery.FieldInfo<>(
            "STRING", (c, w) -> GameConstants.DATE_TIME_FORMATTER.print(Instant.now())));
    return tableConfigure;
  }

  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    ExampleUtils exampleUtils = new ExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        // Read game events from Pub/Sub using custom timestamps, which are extracted from the
        // pubsub data elements, and parse the data.
        .apply(
            PubsubIO.readStrings()
                .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
                .fromTopic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        // Create <team, GameActionInfo> mapping. UpdateTeamScore uses team name as key.
        .apply(
            "MapTeamAsKey",
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(GameActionInfo.class)))
                .via((GameActionInfo gInfo) -> KV.of(gInfo.team, gInfo)))
        // Outputs a team's score every time it passes a new multiple of the threshold.
        .apply("UpdateTeamScore", ParDo.of(new UpdateTeamScoreFn(options.getThresholdScore())))
        // Write the results to BigQuery.
        .apply(
            "WriteTeamLeaders",
            new WriteWindowedToBigQuery<>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getLeaderBoardTableName() + "_team_leader",
                configureCompleteWindowedTableWrite()));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
    exampleUtils.waitToFinish(result);
  }

  /**
   * Tracks each team's score separately in a single state cell and outputs the score every time it
   * passes a new multiple of a threshold.
   *
   * <p>We use stateful {@link DoFn} because:
   *
   * <ul>
   *   <li>State is key-partitioned. Therefore, the score is calculated per team.
   *   <li>Stateful {@link DoFn} can determine when to output based on the state. This only allows
   *       outputting when a team's score passes a given threshold.
   * </ul>
   */
  @VisibleForTesting
  public static class UpdateTeamScoreFn
      extends DoFn<KV<String, GameActionInfo>, KV<String, Integer>> {

    private static final String TOTAL_SCORE = "totalScore";
    private final int thresholdScore;

    public UpdateTeamScoreFn(int thresholdScore) {
      this.thresholdScore = thresholdScore;
    }

    /**
     * Describes the state for storing team score. Let's break down this statement.
     *
     * <p>{@link StateSpec} configures the state cell, which is provided by a runner during pipeline
     * execution.
     *
     * <p>{@link org.apache.beam.sdk.transforms.DoFn.StateId} annotation assigns an identifier to
     * the state, which is used to refer the state in {@link
     * org.apache.beam.sdk.transforms.DoFn.ProcessElement}.
     *
     * <p>A {@link ValueState} stores single value per key and per window. Because our pipeline is
     * globally windowed in this example, this {@link ValueState} is just key partitioned, with one
     * score per team. Any other class that extends {@link org.apache.beam.sdk.state.State} can be
     * used.
     *
     * <p>In order to store the value, the state must be encoded. Therefore, we provide a coder, in
     * this case the {@link VarIntCoder}. If the coder is not provided as in {@code
     * StateSpecs.value()}, Beam's coder inference will try to provide a coder automatically.
     */
    @StateId(TOTAL_SCORE)
    private final StateSpec<ValueState<Integer>> totalScoreSpec =
        StateSpecs.value(VarIntCoder.of());

    /**
     * To use a state cell, annotate a parameter with {@link
     * org.apache.beam.sdk.transforms.DoFn.StateId} that matches the state declaration. The type of
     * the parameter should match the {@link StateSpec} type.
     */
    @ProcessElement
    public void processElement(
        ProcessContext c, @StateId(TOTAL_SCORE) ValueState<Integer> totalScore) {
      String teamName = c.element().getKey();
      GameActionInfo gInfo = c.element().getValue();

      // ValueState cells do not contain a default value. If the state is possibly not written, make
      // sure to check for null on read.
      int oldTotalScore = firstNonNull(totalScore.read(), 0);
      totalScore.write(oldTotalScore + gInfo.score);

      // Since there are no negative scores, the easiest way to check whether a team just passed a
      // new multiple of the threshold score is to compare the quotients of dividing total scores by
      // threshold before and after this aggregation. For example, if the total score was 1999,
      // the new total is 2002, and the threshold is 1000, 1999 / 1000 = 1, 2002 / 1000 = 2.
      // Therefore, this team passed the threshold.
      if (oldTotalScore / this.thresholdScore < totalScore.read() / this.thresholdScore) {
        c.output(KV.of(teamName, totalScore.read()));
      }
    }
  }
}
