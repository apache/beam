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
import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.examples.complete.game.UserScore.ParseEventFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of HourlyTeamScore. Because the pipeline was designed for easy readability and
 * explanations, it lacks good modularity for testing. See our testing documentation for better
 * ideas: https://beam.apache.org/documentation/pipelines/test-your-pipeline/
 */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class HourlyTeamScoreTest implements Serializable {

  @Rule public TestPipeline p = TestPipeline.create();

  /** Test the filtering. */
  @Test
  @Category(ValidatesRunner.class)
  public void testUserScoresFilter() throws Exception {

    List<String> gameEvents = UserScoreTest.getRecordsFromCSVFile("hourly_team_score_game_events.csv");
    List<String> filteredEvents = UserScoreTest.getRecordsFromCSVFile("hourly_team_score_filtered_events.csv");
    List<KV> kvFilteredEvents = new ArrayList<>();

    filteredEvents.forEach(
        s -> {
          List<String> listRow = Splitter.on(',').splitToList(s);
          kvFilteredEvents.add(KV.of(listRow.get(0), Integer.parseInt(listRow.get(1))));
        });

    KV[] akvFilteredEvents = new KV[kvFilteredEvents.size()];

    final Instant startMinTimestamp = new Instant(1447965680000L);

    PCollection<String> input = p.apply(Create.of(gameEvents).withCoder(StringUtf8Coder.of()));

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

    PAssert.that(output).containsInAnyOrder(kvFilteredEvents.toArray(akvFilteredEvents));

    p.run().waitUntilFinish();
  }

  @Test
  public void testUserScoreOptions() {
    PipelineOptionsFactory.as(HourlyTeamScore.Options.class);
  }
}
