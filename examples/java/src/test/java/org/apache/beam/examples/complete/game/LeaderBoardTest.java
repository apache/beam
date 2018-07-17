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

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import org.apache.beam.examples.complete.game.LeaderBoard.CalculateTeamScores;
import org.apache.beam.examples.complete.game.LeaderBoard.CalculateUserScores;
import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LeaderBoard}. */
@RunWith(JUnit4.class)
public class LeaderBoardTest implements Serializable {
  private static final Duration ALLOWED_LATENESS = Duration.standardHours(1);
  private static final Duration TEAM_WINDOW_DURATION = Duration.standardMinutes(20);
  private Instant baseTime = new Instant(0);

  @Rule public TestPipeline p = TestPipeline.create();
  /** Some example users, on two separate teams. */
  private enum TestUser {
    RED_ONE("scarlet", "red"),
    RED_TWO("burgundy", "red"),
    BLUE_ONE("navy", "blue"),
    BLUE_TWO("sky", "blue");

    private final String userName;
    private final String teamName;

    TestUser(String userName, String teamName) {
      this.userName = userName;
      this.teamName = teamName;
    }

    public String getUser() {
      return userName;
    }

    public String getTeam() {
      return teamName;
    }
  }

  /**
   * A test of the {@link CalculateTeamScores} {@link PTransform} when all of the elements arrive on
   * time (ahead of the watermark).
   */
  @Test
  public void testTeamScoresOnTime() {

    TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            // Start at the epoch
            .advanceWatermarkTo(baseTime)
            // add some elements ahead of the watermark
            .addElements(
                event(TestUser.BLUE_ONE, 3, Duration.standardSeconds(3)),
                event(TestUser.BLUE_ONE, 2, Duration.standardMinutes(1)),
                event(TestUser.RED_TWO, 3, Duration.standardSeconds(22)),
                event(TestUser.BLUE_TWO, 5, Duration.standardMinutes(3)))
            // The watermark advances slightly, but not past the end of the window
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
            // Add some more on time elements
            .addElements(
                event(TestUser.RED_ONE, 1, Duration.standardMinutes(4)),
                event(TestUser.BLUE_ONE, 2, Duration.standardSeconds(270)))
            // The window should close and emit an ON_TIME pane
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    String blueTeam = TestUser.BLUE_ONE.getTeam();
    String redTeam = TestUser.RED_ONE.getTeam();
    PAssert.that(teamScores)
        .inOnTimePane(new IntervalWindow(baseTime, TEAM_WINDOW_DURATION))
        .containsInAnyOrder(KV.of(blueTeam, 12), KV.of(redTeam, 4));

    p.run().waitUntilFinish();
  }

  /**
   * A test of the {@link CalculateTeamScores} {@link PTransform} when all of the elements arrive on
   * time, and the processing time advances far enough for speculative panes.
   */
  @Test
  public void testTeamScoresSpeculative() {

    TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            // Start at the epoch
            .advanceWatermarkTo(baseTime)
            .addElements(
                event(TestUser.BLUE_ONE, 3, Duration.standardSeconds(3)),
                event(TestUser.BLUE_ONE, 2, Duration.standardMinutes(1)))
            // Some time passes within the runner, which causes a speculative pane containing the blue
            // team's score to be emitted
            .advanceProcessingTime(Duration.standardMinutes(10))
            .addElements(event(TestUser.RED_TWO, 5, Duration.standardMinutes(3)))
            // Some additional time passes and we get a speculative pane for the red team
            .advanceProcessingTime(Duration.standardMinutes(12))
            .addElements(event(TestUser.BLUE_TWO, 3, Duration.standardSeconds(22)))
            // More time passes and a speculative pane containing a refined value for the blue pane is
            // emitted
            .advanceProcessingTime(Duration.standardMinutes(10))
            // Some more events occur
            .addElements(
                event(TestUser.RED_ONE, 4, Duration.standardMinutes(4)),
                event(TestUser.BLUE_TWO, 2, Duration.standardMinutes(2)))
            // The window closes and we get an ON_TIME pane that contains all of the updates
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    String blueTeam = TestUser.BLUE_ONE.getTeam();
    String redTeam = TestUser.RED_ONE.getTeam();
    IntervalWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);
    // The window contains speculative panes alongside the on-time pane
    PAssert.that(teamScores)
        .inWindow(window)
        .containsInAnyOrder(
            KV.of(blueTeam, 10) /* The on-time blue pane */,
            KV.of(redTeam, 9) /* The on-time red pane */,
            KV.of(blueTeam, 5) /* The first blue speculative pane */,
            KV.of(blueTeam, 8) /* The second blue speculative pane */,
            KV.of(redTeam, 5) /* The red speculative pane */);
    PAssert.that(teamScores)
        .inOnTimePane(window)
        .containsInAnyOrder(KV.of(blueTeam, 10), KV.of(redTeam, 9));

    p.run().waitUntilFinish();
  }

  /**
   * A test where elements arrive behind the watermark (late data), but before the end of the
   * window. These elements are emitted on time.
   */
  @Test
  public void testTeamScoresUnobservablyLate() {

    BoundedWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);
    TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                event(TestUser.BLUE_ONE, 3, Duration.standardSeconds(3)),
                event(TestUser.BLUE_TWO, 5, Duration.standardMinutes(8)),
                event(TestUser.RED_ONE, 4, Duration.standardMinutes(2)),
                event(TestUser.BLUE_ONE, 3, Duration.standardMinutes(5)))
            .advanceWatermarkTo(
                baseTime.plus(TEAM_WINDOW_DURATION).minus(Duration.standardMinutes(1)))
            // These events are late, but the window hasn't closed yet, so the elements are in the
            // on-time pane
            .addElements(
                event(TestUser.RED_TWO, 2, Duration.ZERO),
                event(TestUser.RED_TWO, 5, Duration.standardMinutes(1)),
                event(TestUser.BLUE_TWO, 2, Duration.standardSeconds(90)),
                event(TestUser.RED_TWO, 3, Duration.standardMinutes(3)))
            .advanceWatermarkTo(
                baseTime.plus(TEAM_WINDOW_DURATION).plus(Duration.standardMinutes(1)))
            .advanceWatermarkToInfinity();
    PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    String blueTeam = TestUser.BLUE_ONE.getTeam();
    String redTeam = TestUser.RED_ONE.getTeam();
    // The On Time pane contains the late elements that arrived before the end of the window
    PAssert.that(teamScores)
        .inOnTimePane(window)
        .containsInAnyOrder(KV.of(redTeam, 14), KV.of(blueTeam, 13));

    p.run().waitUntilFinish();
  }

  /**
   * A test where elements arrive behind the watermark (late data) after the watermark passes the
   * end of the window, but before the maximum allowed lateness. These elements are emitted in a
   * late pane.
   */
  @Test
  public void testTeamScoresObservablyLate() {

    Instant firstWindowCloses = baseTime.plus(ALLOWED_LATENESS).plus(TEAM_WINDOW_DURATION);
    TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                event(TestUser.BLUE_ONE, 3, Duration.standardSeconds(3)),
                event(TestUser.BLUE_TWO, 5, Duration.standardMinutes(8)))
            .advanceProcessingTime(Duration.standardMinutes(10))
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
            .addElements(
                event(TestUser.RED_ONE, 3, Duration.standardMinutes(1)),
                event(TestUser.RED_ONE, 4, Duration.standardMinutes(2)),
                event(TestUser.BLUE_ONE, 3, Duration.standardMinutes(5)))
            .advanceWatermarkTo(firstWindowCloses.minus(Duration.standardMinutes(1)))
            // These events are late but should still appear in a late pane
            .addElements(
                event(TestUser.RED_TWO, 2, Duration.ZERO),
                event(TestUser.RED_TWO, 5, Duration.standardMinutes(1)),
                event(TestUser.RED_TWO, 3, Duration.standardMinutes(3)))
            // A late refinement is emitted due to the advance in processing time, but the window has
            // not yet closed because the watermark has not advanced
            .advanceProcessingTime(Duration.standardMinutes(12))
            // These elements should appear in the final pane
            .addElements(
                event(TestUser.RED_TWO, 9, Duration.standardMinutes(1)),
                event(TestUser.RED_TWO, 1, Duration.standardMinutes(3)))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    BoundedWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);
    String blueTeam = TestUser.BLUE_ONE.getTeam();
    String redTeam = TestUser.RED_ONE.getTeam();
    PAssert.that(teamScores)
        .inWindow(window)
        .satisfies(
            input -> {
              // The final sums need not exist in the same pane, but must appear in the output
              // PCollection
              assertThat(input, hasItem(KV.of(blueTeam, 11)));
              assertThat(input, hasItem(KV.of(redTeam, 27)));
              return null;
            });
    PAssert.thatMap(teamScores)
        // The closing behavior of CalculateTeamScores precludes an inFinalPane matcher
        .inOnTimePane(window)
        .isEqualTo(
            ImmutableMap.<String, Integer>builder().put(redTeam, 7).put(blueTeam, 11).build());

    // No final pane is emitted for the blue team, as all of their updates have been taken into
    // account in earlier panes
    PAssert.that(teamScores).inFinalPane(window).containsInAnyOrder(KV.of(redTeam, 27));

    p.run().waitUntilFinish();
  }

  /**
   * A test where elements arrive beyond the maximum allowed lateness. These elements are dropped
   * within {@link CalculateTeamScores} and do not impact the final result.
   */
  @Test
  public void testTeamScoresDroppablyLate() {

    BoundedWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);
    TestStream<GameActionInfo> infos =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .addElements(
                event(TestUser.BLUE_ONE, 12, Duration.ZERO),
                event(TestUser.RED_ONE, 3, Duration.ZERO))
            .advanceWatermarkTo(window.maxTimestamp())
            .addElements(
                event(TestUser.RED_ONE, 4, Duration.standardMinutes(2)),
                event(TestUser.BLUE_TWO, 3, Duration.ZERO),
                event(TestUser.BLUE_ONE, 3, Duration.standardMinutes(3)))
            // Move the watermark to the end of the window to output on time
            .advanceWatermarkTo(baseTime.plus(TEAM_WINDOW_DURATION))
            // Move the watermark past the end of the allowed lateness plus the end of the window
            .advanceWatermarkTo(
                baseTime
                    .plus(ALLOWED_LATENESS)
                    .plus(TEAM_WINDOW_DURATION)
                    .plus(Duration.standardMinutes(1)))
            // These elements within the expired window are droppably late, and will not appear in the
            // output
            .addElements(
                event(
                    TestUser.BLUE_TWO, 3, TEAM_WINDOW_DURATION.minus(Duration.standardSeconds(5))),
                event(TestUser.RED_ONE, 7, Duration.standardMinutes(4)))
            .advanceWatermarkToInfinity();
    PCollection<KV<String, Integer>> teamScores =
        p.apply(infos).apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    String blueTeam = TestUser.BLUE_ONE.getTeam();
    String redTeam = TestUser.RED_ONE.getTeam();
    // Only one on-time pane and no late panes should be emitted
    PAssert.that(teamScores)
        .inWindow(window)
        .containsInAnyOrder(KV.of(redTeam, 7), KV.of(blueTeam, 18));
    // No elements are added before the watermark passes the end of the window plus the allowed
    // lateness, so no refinement should be emitted
    PAssert.that(teamScores).inFinalPane(window).empty();

    p.run().waitUntilFinish();
  }

  /**
   * A test where elements arrive both on-time and late in {@link CalculateUserScores}, which emits
   * output into the {@link GlobalWindow}. All elements that arrive should be taken into account,
   * even if they arrive later than the maximum allowed lateness.
   */
  @Test
  public void testUserScore() {

    TestStream<GameActionInfo> infos =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .addElements(
                event(TestUser.BLUE_ONE, 12, Duration.ZERO),
                event(TestUser.RED_ONE, 3, Duration.ZERO))
            .advanceProcessingTime(Duration.standardMinutes(7))
            .addElements(
                event(TestUser.RED_ONE, 4, Duration.standardMinutes(2)),
                event(TestUser.BLUE_TWO, 3, Duration.ZERO),
                event(TestUser.BLUE_ONE, 3, Duration.standardMinutes(3)))
            .advanceProcessingTime(Duration.standardMinutes(5))
            .advanceWatermarkTo(baseTime.plus(ALLOWED_LATENESS).plus(Duration.standardHours(12)))
            // Late elements are always observable within the global window - they arrive before
            // the window closes, so they will appear in a pane, even if they arrive after the
            // allowed lateness, and are taken into account alongside on-time elements
            .addElements(
                event(TestUser.RED_ONE, 3, Duration.standardMinutes(7)),
                event(TestUser.RED_ONE, 2, (ALLOWED_LATENESS).plus(Duration.standardHours(13))))
            .advanceProcessingTime(Duration.standardMinutes(6))
            .addElements(event(TestUser.BLUE_TWO, 5, Duration.standardMinutes(12)))
            .advanceProcessingTime(Duration.standardMinutes(20))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Integer>> userScores =
        p.apply(infos).apply(new CalculateUserScores(ALLOWED_LATENESS));

    // User scores are emitted in speculative panes in the Global Window - this matcher choice
    // ensures that panes emitted by the watermark advancing to positive infinity are not included,
    // as that will not occur outside of tests
    PAssert.that(userScores)
        .inEarlyGlobalWindowPanes()
        .containsInAnyOrder(
            KV.of(TestUser.BLUE_ONE.getUser(), 15),
            KV.of(TestUser.RED_ONE.getUser(), 7),
            KV.of(TestUser.RED_ONE.getUser(), 12),
            KV.of(TestUser.BLUE_TWO.getUser(), 3),
            KV.of(TestUser.BLUE_TWO.getUser(), 8));

    p.run().waitUntilFinish();
  }

  @Test
  public void testLeaderBoardOptions() {
    PipelineOptionsFactory.as(LeaderBoard.Options.class);
  }

  private TimestampedValue<GameActionInfo> event(
      TestUser user, int score, Duration baseTimeOffset) {
    return TimestampedValue.of(
        new GameActionInfo(
            user.getUser(), user.getTeam(), score, baseTime.plus(baseTimeOffset).getMillis()),
        baseTime.plus(baseTimeOffset));
  }
}
