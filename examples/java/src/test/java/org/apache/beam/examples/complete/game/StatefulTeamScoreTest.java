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

import org.apache.beam.examples.complete.game.StatefulTeamScore.UpdateTeamScoreFn;
import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StatefulTeamScore}. */
@RunWith(JUnit4.class)
public class StatefulTeamScoreTest {

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
   * Tests that {@link UpdateTeamScoreFn} {@link org.apache.beam.sdk.transforms.DoFn} outputs
   * correctly for one team.
   */
  @Test
  public void testScoreUpdatesOneTeam() {

    TestStream<KV<String, GameActionInfo>> createEvents =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(GameActionInfo.class)))
            .advanceWatermarkTo(baseTime)
            .addElements(
                event(TestUser.RED_TWO, 99, Duration.standardSeconds(10)),
                event(TestUser.RED_ONE, 1, Duration.standardSeconds(20)),
                event(TestUser.RED_ONE, 0, Duration.standardSeconds(30)),
                event(TestUser.RED_TWO, 100, Duration.standardSeconds(40)),
                event(TestUser.RED_TWO, 201, Duration.standardSeconds(50)))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents).apply(ParDo.of(new UpdateTeamScoreFn(100)));

    String redTeam = TestUser.RED_ONE.getTeam();

    PAssert.that(teamScores)
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(KV.of(redTeam, 100), KV.of(redTeam, 200), KV.of(redTeam, 401));

    p.run().waitUntilFinish();
  }

  /**
   * Tests that {@link UpdateTeamScoreFn} {@link org.apache.beam.sdk.transforms.DoFn} outputs
   * correctly for multiple teams.
   */
  @Test
  public void testScoreUpdatesPerTeam() {

    TestStream<KV<String, GameActionInfo>> createEvents =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(GameActionInfo.class)))
            .advanceWatermarkTo(baseTime)
            .addElements(
                event(TestUser.RED_ONE, 50, Duration.standardSeconds(10)),
                event(TestUser.RED_TWO, 50, Duration.standardSeconds(20)),
                event(TestUser.BLUE_ONE, 70, Duration.standardSeconds(30)),
                event(TestUser.BLUE_TWO, 80, Duration.standardSeconds(40)),
                event(TestUser.BLUE_TWO, 50, Duration.standardSeconds(50)))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents).apply(ParDo.of(new UpdateTeamScoreFn(100)));

    String redTeam = TestUser.RED_ONE.getTeam();
    String blueTeam = TestUser.BLUE_ONE.getTeam();

    PAssert.that(teamScores)
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(KV.of(redTeam, 100), KV.of(blueTeam, 150), KV.of(blueTeam, 200));

    p.run().waitUntilFinish();
  }

  /**
   * Tests that {@link UpdateTeamScoreFn} {@link org.apache.beam.sdk.transforms.DoFn} outputs
   * correctly per window and per key.
   */
  @Test
  public void testScoreUpdatesPerWindow() {

    TestStream<KV<String, GameActionInfo>> createEvents =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(GameActionInfo.class)))
            .advanceWatermarkTo(baseTime)
            .addElements(
                event(TestUser.RED_ONE, 50, Duration.standardMinutes(1)),
                event(TestUser.RED_TWO, 50, Duration.standardMinutes(2)),
                event(TestUser.RED_ONE, 50, Duration.standardMinutes(3)),
                event(TestUser.RED_ONE, 60, Duration.standardMinutes(6)),
                event(TestUser.RED_TWO, 60, Duration.standardMinutes(7)))
            .advanceWatermarkToInfinity();

    Duration teamWindowDuration = Duration.standardMinutes(5);

    PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(Window.<KV<String, GameActionInfo>>into(FixedWindows.of(teamWindowDuration)))
            .apply(ParDo.of(new UpdateTeamScoreFn(100)));

    String redTeam = TestUser.RED_ONE.getTeam();
    String blueTeam = TestUser.BLUE_ONE.getTeam();

    IntervalWindow window1 = new IntervalWindow(baseTime, teamWindowDuration);
    IntervalWindow window2 = new IntervalWindow(window1.end(), teamWindowDuration);

    PAssert.that(teamScores).inWindow(window1).containsInAnyOrder(KV.of(redTeam, 100));

    PAssert.that(teamScores).inWindow(window2).containsInAnyOrder(KV.of(redTeam, 120));

    p.run().waitUntilFinish();
  }

  private TimestampedValue<KV<String, GameActionInfo>> event(
      TestUser user, int score, Duration baseTimeOffset) {
    return TimestampedValue.of(
        KV.of(
            user.getTeam(),
            new GameActionInfo(
                user.getUser(), user.getTeam(), score, baseTime.plus(baseTimeOffset).getMillis())),
        baseTime.plus(baseTimeOffset));
  }
}
