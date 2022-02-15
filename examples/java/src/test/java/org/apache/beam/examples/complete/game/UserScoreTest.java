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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Splitter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.beam.examples.complete.game.UserScore.ExtractAndSumScore;
import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.examples.complete.game.UserScore.ParseEventFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of UserScore. */
@RunWith(JUnit4.class)
public class UserScoreTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(UserScoreTest.class);

  static List<String> GAME_EVENTS = getRecordsFromCSVFile("user_score_test_events1.csv");
  static List<String> GAME_EVENTS2 = getRecordsFromCSVFile("user_score_test_events2.csv");
  static List<String> USER_SUMS = getRecordsFromCSVFile("user_score_test_user_sums.csv");
  static List<String> TEAM_SUMS = getRecordsFromCSVFile("user_score_test_team_sums.csv");

  @Rule public TestPipeline p = TestPipeline.create();

  /** Test the {@link ParseEventFn} {@link org.apache.beam.sdk.transforms.DoFn}. */
  @Test
  public void testParseEventFn() throws Exception {
    PCollection<String> input = p.apply(Create.of(GAME_EVENTS));
    PCollection<GameActionInfo> output = input.apply(ParDo.of(new ParseEventFn()));

    List<GameActionInfo> gameActionInfoList = Lists.newArrayList();

    GAME_EVENTS.forEach(
        s -> {
          List<String> listRow = Splitter.on(',').splitToList(s);

          if (listRow.get(0).equals("user6_AliceBlueDingo")
              || listRow.get(0).equals("THIS IS A PARSE ERROR")) return;

          gameActionInfoList.add(
              new GameActionInfo(
                  listRow.get(0),
                  listRow.get(1),
                  Integer.valueOf(listRow.get(2)),
                  Long.valueOf(listRow.get(3))));
        });

    PAssert.that(output).containsInAnyOrder(gameActionInfoList);

    p.run().waitUntilFinish();
  }

  /** Tests ExtractAndSumScore("user"). */
  @Test
  @Category(ValidatesRunner.class)
  public void testUserScoreSums() throws Exception {

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS));

    PCollection<KV<String, Integer>> output =
        input
            .apply(ParDo.of(new ParseEventFn()))
            // Extract and sum username/score pairs from the event data.
            .apply("ExtractUserScore", new ExtractAndSumScore("user"));

    List<KV<String, Integer>> kvUserSums = getKVs(USER_SUMS);

    // Check the user score sums.
    PAssert.that(output).containsInAnyOrder(kvUserSums);

    p.run().waitUntilFinish();
  }

  /** Tests ExtractAndSumScore("team"). */
  @Test
  @Category(ValidatesRunner.class)
  public void testTeamScoreSums() throws Exception {

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS));

    PCollection<KV<String, Integer>> output =
        input
            .apply(ParDo.of(new ParseEventFn()))
            // Extract and sum teamname/score pairs from the event data.
            .apply("ExtractTeamScore", new ExtractAndSumScore("team"));

    List<KV<String, Integer>> kvTeamSums = getKVs(TEAM_SUMS);

    // Check the team score sums.
    PAssert.that(output).containsInAnyOrder(kvTeamSums);

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

  private static List<String> getRecordsFromCSVFile(String filePath) {

    String resourcesDir = "./";

    String file = Resources.getResource(resourcesDir + filePath).getPath();

    List<String> values = new ArrayList<>();
    Scanner lineScanner = null;

    try {
      lineScanner = new Scanner(new File(file), UTF_8.name());
    } catch (FileNotFoundException e) {
      LOG.error(e.getMessage());
    }

    lineScanner.useDelimiter(System.lineSeparator());

    while (lineScanner.hasNext()) {
      values.add(lineScanner.next());
    }

    return values;
  }

  private List<KV<String, Integer>> getKVs(List<String> records) {
    List<KV<String, Integer>> sums = new ArrayList<>();

    records.forEach(
        s -> {
          List<String> listRow = Splitter.on(',').splitToList(s);
          sums.add(KV.of(listRow.get(0), Integer.parseInt(listRow.get(1))));
        });
    return sums;
  }
}
