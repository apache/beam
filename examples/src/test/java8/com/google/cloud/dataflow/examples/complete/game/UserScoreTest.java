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

package com.google.cloud.dataflow.examples.complete.game;

import com.google.cloud.dataflow.examples.complete.game.UserScore.ExtractAndSumScore;
import com.google.cloud.dataflow.examples.complete.game.UserScore.GameActionInfo;
import com.google.cloud.dataflow.examples.complete.game.UserScore.ParseEventFn;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Tests of UserScore.
 */
@RunWith(JUnit4.class)
public class UserScoreTest implements Serializable {

  static final String[] GAME_EVENTS_ARRAY = new String[] {
    "user0_MagentaKangaroo,MagentaKangaroo,3,1447955630000,2015-11-19 09:53:53.444",
    "user13_ApricotQuokka,ApricotQuokka,15,1447955630000,2015-11-19 09:53:53.444",
    "user6_AmberNumbat,AmberNumbat,11,1447955630000,2015-11-19 09:53:53.444",
    "user7_AlmondWallaby,AlmondWallaby,15,1447955630000,2015-11-19 09:53:53.444",
    "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,12,1447955630000,2015-11-19 09:53:53.444",
    "user6_AliceBlueDingo,AliceBlueDingo,4,xxxxxxx,2015-11-19 09:53:53.444",
    "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,11,1447955630000,2015-11-19 09:53:53.444",
    "THIS IS A PARSE ERROR,2015-11-19 09:53:53.444",
    "user19_BisqueBilby,BisqueBilby,6,1447955630000,2015-11-19 09:53:53.444",
    "user19_BisqueBilby,BisqueBilby,8,1447955630000,2015-11-19 09:53:53.444"
  };

    static final String[] GAME_EVENTS_ARRAY2 = new String[] {
    "user6_AliceBlueDingo,AliceBlueDingo,4,xxxxxxx,2015-11-19 09:53:53.444",
    "THIS IS A PARSE ERROR,2015-11-19 09:53:53.444",
    "user13_BisqueBilby,BisqueBilby,xxx,1447955630000,2015-11-19 09:53:53.444"
  };

  static final List<String> GAME_EVENTS = Arrays.asList(GAME_EVENTS_ARRAY);
  static final List<String> GAME_EVENTS2 = Arrays.asList(GAME_EVENTS_ARRAY2);

  static final KV[] USER_SUMS = new KV[] {
      KV.of("user0_MagentaKangaroo", 3), KV.of("user13_ApricotQuokka", 15),
      KV.of("user6_AmberNumbat", 11), KV.of("user7_AlmondWallaby", 15),
      KV.of("user7_AndroidGreenKookaburra", 23),
      KV.of("user19_BisqueBilby", 14) };

  static final KV[] TEAM_SUMS = new KV[] {
      KV.of("MagentaKangaroo", 3), KV.of("ApricotQuokka", 15),
      KV.of("AmberNumbat", 11), KV.of("AlmondWallaby", 15),
      KV.of("AndroidGreenKookaburra", 23),
      KV.of("BisqueBilby", 14) };

  /** Test the ParseEventFn DoFn. */
  @Test
  public void testParseEventFn() {
    DoFnTester<String, GameActionInfo> parseEventFn =
        DoFnTester.of(new ParseEventFn());

    List<GameActionInfo> results = parseEventFn.processBatch(GAME_EVENTS_ARRAY);
    Assert.assertEquals(results.size(), 8);
    Assert.assertEquals(results.get(0).getUser(), "user0_MagentaKangaroo");
    Assert.assertEquals(results.get(0).getTeam(), "MagentaKangaroo");
    Assert.assertEquals(results.get(0).getScore(), new Integer(3));
  }

  /** Tests ExtractAndSumScore("user"). */
  @Test
  @Category(RunnableOnService.class)
  public void testUserScoreSums() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> output = input
      .apply(ParDo.of(new ParseEventFn()))
      // Extract and sum username/score pairs from the event data.
      .apply("ExtractUserScore", new ExtractAndSumScore("user"));

    // Check the user score sums.
    DataflowAssert.that(output).containsInAnyOrder(USER_SUMS);

    p.run();
  }

  /** Tests ExtractAndSumScore("team"). */
  @Test
  @Category(RunnableOnService.class)
  public void testTeamScoreSums() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> output = input
      .apply(ParDo.of(new ParseEventFn()))
      // Extract and sum teamname/score pairs from the event data.
      .apply("ExtractTeamScore", new ExtractAndSumScore("team"));

    // Check the team score sums.
    DataflowAssert.that(output).containsInAnyOrder(TEAM_SUMS);

    p.run();
  }

  /** Test that bad input data is dropped appropriately. */
  @Test
  @Category(RunnableOnService.class)
  public void testUserScoresBadInput() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS2).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> extract = input
      .apply(ParDo.of(new ParseEventFn()))
      .apply(
          MapElements.via((GameActionInfo gInfo) -> KV.of(gInfo.getUser(), gInfo.getScore()))
          .withOutputType(new TypeDescriptor<KV<String, Integer>>() {}));

    DataflowAssert.that(extract).empty();

    p.run();
  }
}
