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

import com.google.api.services.bigquery.model.*;
import com.google.api.services.pubsub.Pubsub;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.beam.examples.complete.game.injector.InjectorUtils;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link LeaderBoard}. */
@RunWith(JUnit4.class)
public class LeaderBoardIT {
  private LeaderBoardOptions options;
  private static Pubsub pubsub;
  private static String projectId;
  private static final String TOPIC = "projects/apache-beam-testing/topics/leaderboardscores";
  private static BigqueryClient bqClient;
  private static final String outputDataset = "leader_board_e2e_events";
  static List<String> GAME_EVENTS = getRecordsFromCSVFile("leaderboard_test_events.csv");
  private static final Logger LOG = LoggerFactory.getLogger(LeaderBoardIT.class);
  PipelineOptions pipelineOptions =
      PipelineOptionsFactory.fromArgs(
              "--tempLocation=apache-beam-testing:leader_board_e2e_events.leaderboard_team_scores")
          .create();
  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(pipelineOptions);

  private static final TableSchema NEW_TYPES_QUERY_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("total_score").setType("INTEGER"),
                  new TableFieldSchema().setName("team").setType("STRING")));

  public interface LeaderBoardOptions extends TestPipelineOptions, LeaderBoard.Options {};

  @Before
  public void setupTestEnvironment() throws Exception {

    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setupBigQuery();

    setupPubSub();

    setupPipelineOptions();
  }

  @Test
  public void testE2ELeaderBoard() throws Exception {

    LeaderBoard.runLeaderBoard(options, p);

    PCollection<Integer> scores;

    scores =
        p.apply(
                BigQueryIO.readTableRows()
                    .from("apache-beam-testing:leader_board_e2e_events.leaderboard_team"))
            .apply(ParDo.of(new ExtractTeamsScores()))
            .apply(ParDo.of(new ExtractScoreByTeam("MagentaKangaroo")));

    p.run().waitUntilFinish();

    PAssert.that(scores).containsInAnyOrder(18);
  }

  static class ExtractTeamsScores extends DoFn<TableRow, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      String team = (String) row.get("team");
      Integer score = Integer.parseInt(row.get("total_score").toString());
      c.output(KV.of(team, score));
    }
  }

  static class ExtractScoreByTeam extends DoFn<KV<String, Integer>, Integer> {

    private String teamName;

    public ExtractScoreByTeam(String teamName) {
      this.teamName = teamName;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Integer> row = c.element();

      if (row.getValue().toString().matches(this.teamName))
        c.output(Integer.parseInt(row.getValue().toString()));
    }
  }

  @After
  public void cleanupTestEnvironment() throws Exception {
    bqClient.deleteDataset(projectId, outputDataset);
    pubsub.projects().topics().delete(TOPIC);
  }

  private void setupBigQuery() throws IOException, InterruptedException {
    bqClient = new BigqueryClient("LeaderBoardIT");
    bqClient.createNewDataset(projectId, outputDataset);
    bqClient.createNewTable(
        projectId,
        outputDataset,
        new Table()
            .setSchema(NEW_TYPES_QUERY_TABLE_SCHEMA)
            .setTableReference(
                new TableReference()
                    .setTableId("leaderboard_team_scores")
                    .setDatasetId(outputDataset)
                    .setProjectId(projectId)));
  }

  private void setupPubSub() throws IOException {
    pubsub = InjectorUtils.getClient();
    InjectorUtils.createTopic(pubsub, TOPIC);

    PubsubIO.Write<String> write =
        PubsubIO.writeStrings()
            .to(TOPIC)
            .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
            .withIdAttribute(projectId);

    Create.of(GAME_EVENTS);

    p.apply(Create.of(GAME_EVENTS)).apply(write);
  }

  private void setupPipelineOptions() {
    options = TestPipeline.testingPipelineOptions().as(LeaderBoardOptions.class);
    options.as(GcpOptions.class).setProject(projectId);
    // options.as(BigQueryOptions.class).setTempLocation("apache-beam-testing:leader_board_e2e_events.leaderboard_team_scores");
    options.setDataset(outputDataset);
    options.setTopic(TOPIC);
    options.setStreaming(true);
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
}
