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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.examples.complete.game.UserScore.ExtractAndSumScore;
import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.examples.complete.game.UserScore.ParseEventFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of UserScore. */
@RunWith(JUnit4.class)
public class UserScoreTest implements Serializable {

  static final String[] GAME_EVENTS_ARRAY =
      new String[] {
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

  static final String[] GAME_EVENTS_ARRAY2 =
      new String[] {
        "user6_AliceBlueDingo,AliceBlueDingo,4,xxxxxxx,2015-11-19 09:53:53.444",
        "THIS IS A PARSE ERROR,2015-11-19 09:53:53.444",
        "user13_BisqueBilby,BisqueBilby,xxx,1447955630000,2015-11-19 09:53:53.444"
      };

  static final List<String> GAME_EVENTS = Arrays.asList(GAME_EVENTS_ARRAY);
  static final List<String> GAME_EVENTS2 = Arrays.asList(GAME_EVENTS_ARRAY2);

  static final List<KV<String, Integer>> USER_SUMS =
      Arrays.asList(
          KV.of("user0_MagentaKangaroo", 3),
          KV.of("user13_ApricotQuokka", 15),
          KV.of("user6_AmberNumbat", 11),
          KV.of("user7_AlmondWallaby", 15),
          KV.of("user7_AndroidGreenKookaburra", 23),
          KV.of("user19_BisqueBilby", 14));

  static final List<KV<String, Integer>> TEAM_SUMS =
      Arrays.asList(
          KV.of("MagentaKangaroo", 3),
          KV.of("ApricotQuokka", 15),
          KV.of("AmberNumbat", 11),
          KV.of("AlmondWallaby", 15),
          KV.of("AndroidGreenKookaburra", 23),
          KV.of("BisqueBilby", 14));

  @Rule public TestPipeline p = TestPipeline.create();

  /** Test the {@link ParseEventFn} {@link org.apache.beam.sdk.transforms.DoFn}. */
  @Test
  public void testParseEventFn() throws Exception {
    DoFnTester<String, GameActionInfo> parseEventFn = DoFnTester.of(new ParseEventFn());

    List<GameActionInfo> results = parseEventFn.processBundle(GAME_EVENTS_ARRAY);
    Assert.assertEquals(8, results.size());
    Assert.assertEquals("user0_MagentaKangaroo", results.get(0).getUser());
    Assert.assertEquals("MagentaKangaroo", results.get(0).getTeam());
    Assert.assertEquals(Integer.valueOf(3), results.get(0).getScore());
  }

  /** Tests ExtractAndSumScore("user"). */
  @Test
  @Category(ValidatesRunner.class)
  public void testUserScoreSums() throws Exception {

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> output =
        input
            .apply(ParDo.of(new ParseEventFn()))
            // Extract and sum username/score pairs from the event data.
            .apply("ExtractUserScore", new ExtractAndSumScore("user"));

    // Check the user score sums.
    PAssert.that(output).containsInAnyOrder(USER_SUMS);

    p.run().waitUntilFinish();
  }

  /** Tests ExtractAndSumScore("team"). */
  @Test
  @Category(ValidatesRunner.class)
  public void testTeamScoreSums() throws Exception {

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> output =
        input
            .apply(ParDo.of(new ParseEventFn()))
            // Extract and sum teamname/score pairs from the event data.
            .apply("ExtractTeamScore", new ExtractAndSumScore("team"));

    // Check the team score sums.
    PAssert.that(output).containsInAnyOrder(TEAM_SUMS);

    p.run().waitUntilFinish();
  }

  /** Test that bad input data is dropped appropriately. */
  @Test
  @Category(ValidatesRunner.class)
  public void testUserScoresBadInput() throws Exception {

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS2).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> extract =
        input
            .apply(ParDo.of(new ParseEventFn()))
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .via((GameActionInfo gInfo) -> KV.of(gInfo.getUser(), gInfo.getScore())));

    PAssert.that(extract).empty();

    p.run().waitUntilFinish();
  }
}
