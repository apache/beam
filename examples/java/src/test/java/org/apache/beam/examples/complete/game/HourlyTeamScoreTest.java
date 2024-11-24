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
import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.examples.complete.game.UserScore.ParseEventFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of HourlyTeamScore. Because the pipeline was designed for easy readability and
 * explanations, it lacks good modularity for testing. See our testing documentation for better
 * ideas: https://beam.apache.org/documentation/pipelines/test-your-pipeline/
 */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class HourlyTeamScoreTest implements Serializable {

  static final String[] GAME_EVENTS_ARRAY =
      new String[] {
        "user0_MagentaKangaroo,MagentaKangaroo,3,1447955630000,2015-11-19 09:53:53.444",
        "user13_ApricotQuokka,ApricotQuokka,15,1447955630000,2015-11-19 09:53:53.444",
        "user6_AmberNumbat,AmberNumbat,11,1447955630000,2015-11-19 09:53:53.444",
        "user7_AlmondWallaby,AlmondWallaby,15,1447955630000,2015-11-19 09:53:53.444",
        "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,12,1447955630000,2015-11-19 09:53:53.444",
        "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,11,1447955630000,2015-11-19 09:53:53.444",
        "user19_BisqueBilby,BisqueBilby,6,1447955630000,2015-11-19 09:53:53.444",
        "user19_BisqueBilby,BisqueBilby,8,1447955630000,2015-11-19 09:53:53.444",
        // time gap...
        "user0_AndroidGreenEchidna,AndroidGreenEchidna,0,1447965690000,2015-11-19 12:41:31.053",
        "user0_MagentaKangaroo,MagentaKangaroo,4,1447965690000,2015-11-19 12:41:31.053",
        "user2_AmberCockatoo,AmberCockatoo,13,1447965690000,2015-11-19 12:41:31.053",
        "user18_BananaEmu,BananaEmu,7,1447965690000,2015-11-19 12:41:31.053",
        "user3_BananaEmu,BananaEmu,17,1447965690000,2015-11-19 12:41:31.053",
        "user18_BananaEmu,BananaEmu,1,1447965690000,2015-11-19 12:41:31.053",
        "user18_ApricotCaneToad,ApricotCaneToad,14,1447965690000,2015-11-19 12:41:31.053"
      };

  static final List<String> GAME_EVENTS = Arrays.asList(GAME_EVENTS_ARRAY);

  // Used to check the filtering.
  static final KV[] FILTERED_EVENTS =
      new KV[] {
        KV.of("user0_AndroidGreenEchidna", 0),
        KV.of("user0_MagentaKangaroo", 4),
        KV.of("user2_AmberCockatoo", 13),
        KV.of("user18_BananaEmu", 7),
        KV.of("user3_BananaEmu", 17),
        KV.of("user18_BananaEmu", 1),
        KV.of("user18_ApricotCaneToad", 14)
      };

  @Rule public TestPipeline p = TestPipeline.create();

  /** Test the filtering. */
  @Test
  public void testUserScoresFilter() throws Exception {

    final Instant startMinTimestamp = new Instant(1447965680000L);

    PCollection<String> input = p.apply(Create.of(GAME_EVENTS).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> output =
        input
            .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
            .apply(
                "FilterStartTime",
                Filter.by(
                    (GameActionInfo gInfo) -> gInfo.getTimestamp() > startMinTimestamp.getMillis()))
            // run a map to access the fields in the result.
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .via((GameActionInfo gInfo) -> KV.of(gInfo.getUser(), gInfo.getScore())));

    PAssert.that(output).containsInAnyOrder(FILTERED_EVENTS);

    p.run().waitUntilFinish();
  }

  @Test
  public void testUserScoreOptions() {
    PipelineOptionsFactory.as(HourlyTeamScore.Options.class);
  }
}
