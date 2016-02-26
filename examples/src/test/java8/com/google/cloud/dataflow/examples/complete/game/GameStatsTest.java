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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.examples.complete.game.GameStats.CalculateSpammyUsers;
import com.google.cloud.dataflow.examples.complete.game.UserScore.GameActionInfo;
import com.google.cloud.dataflow.examples.complete.game.UserScore.ParseEventFn;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Tests of GameStats.
 * Because the pipeline was designed for easy readability and explanations, it lacks good
 * modularity for testing. See our testing documentation for better ideas:
 * https://cloud.google.com/dataflow/pipelines/testing-your-pipeline.
 */
@RunWith(JUnit4.class)
public class GameStatsTest implements Serializable {

  // User scores
  static final KV<String, Integer>[] USER_SCORES_ARRAY = new KV[] {
    KV.of("Robot-2", 66), KV.of("Robot-1", 116), KV.of("user7_AndroidGreenKookaburra", 23),
    KV.of("user7_AndroidGreenKookaburra", 1),
    KV.of("user19_BisqueBilby", 14), KV.of("user13_ApricotQuokka", 15),
    KV.of("user18_BananaEmu", 25), KV.of("user6_AmberEchidna", 8),
    KV.of("user2_AmberQuokka", 6), KV.of("user0_MagentaKangaroo", 4),
    KV.of("user0_MagentaKangaroo", 3), KV.of("user2_AmberCockatoo", 13),
    KV.of("user7_AlmondWallaby", 15), KV.of("user6_AmberNumbat", 11),
    KV.of("user6_AmberQuokka", 4)
  };

  static final List<KV<String, Integer>> USER_SCORES = Arrays.asList(USER_SCORES_ARRAY);

  // The expected list of 'spammers'.
  static final KV[] SPAMMERS = new KV[] {
      KV.of("Robot-2", 66), KV.of("Robot-1", 116)
    };


  /** Test the calculation of 'spammy users'. */
  @Test
  @Category(RunnableOnService.class)
  public void testCalculateSpammyUsers() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input = p.apply(Create.of(USER_SCORES));
    PCollection<KV<String, Integer>> output = input.apply(new CalculateSpammyUsers());

    // Check the set of spammers.
    DataflowAssert.that(output).containsInAnyOrder(SPAMMERS);

    p.run();
  }

}
