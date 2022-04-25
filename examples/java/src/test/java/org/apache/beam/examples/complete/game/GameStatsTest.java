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

// beam-playground:
//   name: GameStatsTest
//   description: Unit-test for the GameStats example.
//   multifile: false
//   context_line: 51
//   categories:
//     - Testing
//     - Filtering

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.examples.complete.game.GameStats.CalculateSpammyUsers;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of GameStats. Because the pipeline was designed for easy readability and explanations, it
 * lacks good modularity for testing. See our testing documentation for better ideas:
 * https://beam.apache.org/documentation/pipelines/test-your-pipeline/
 */
@RunWith(JUnit4.class)
public class GameStatsTest implements Serializable {

  // User scores
  static final List<KV<String, Integer>> USER_SCORES =
      Arrays.asList(
          KV.of("Robot-2", 66),
          KV.of("Robot-1", 116),
          KV.of("user7_AndroidGreenKookaburra", 23),
          KV.of("user7_AndroidGreenKookaburra", 1),
          KV.of("user19_BisqueBilby", 14),
          KV.of("user13_ApricotQuokka", 15),
          KV.of("user18_BananaEmu", 25),
          KV.of("user6_AmberEchidna", 8),
          KV.of("user2_AmberQuokka", 6),
          KV.of("user0_MagentaKangaroo", 4),
          KV.of("user0_MagentaKangaroo", 3),
          KV.of("user2_AmberCockatoo", 13),
          KV.of("user7_AlmondWallaby", 15),
          KV.of("user6_AmberNumbat", 11),
          KV.of("user6_AmberQuokka", 4));

  // The expected list of 'spammers'.
  static final List<KV<String, Integer>> SPAMMERS =
      Arrays.asList(KV.of("Robot-2", 66), KV.of("Robot-1", 116));

  @Rule public TestPipeline p = TestPipeline.create();

  /** Test the calculation of 'spammy users'. */
  @Test
  @Category(ValidatesRunner.class)
  public void testCalculateSpammyUsers() throws Exception {
    PCollection<KV<String, Integer>> input = p.apply(Create.of(USER_SCORES));
    PCollection<KV<String, Integer>> output = input.apply(new CalculateSpammyUsers());

    // Check the set of spammers.
    PAssert.that(output).containsInAnyOrder(SPAMMERS);

    p.run().waitUntilFinish();
  }

  @Test
  public void testGameStatsOptions() {
    PipelineOptionsFactory.as(GameStats.Options.class);
  }
}
