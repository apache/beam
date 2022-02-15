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

import com.google.api.services.pubsub.Pubsub;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.beam.examples.complete.game.injector.InjectorUtils;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
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
  private String projectId;
  private static String topic;
  private BigqueryClient bqClient;
  private final String outputDataset = "leader_board_e2e_events";
  static List<String> GAME_EVENTS = getRecordsFromCSVFile("leaderboard_test_events.csv");
  private static final Logger LOG = LoggerFactory.getLogger(LeaderBoardIT.class);
  @Rule public TestPipeline p = TestPipeline.create();

  public interface LeaderBoardOptions extends TestPipelineOptions, LeaderBoard.Options {};

  @Before
  public void setupTestEnvironment() throws Exception {
    PCollection<String> input = p.apply(Create.of(GAME_EVENTS));

    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    bqClient = new BigqueryClient("LeaderBoardIT");
    bqClient.createNewDataset(projectId, outputDataset);

    pubsub = InjectorUtils.getClient();
    topic = InjectorUtils.getFullyQualifiedTopicName(projectId, LeaderBoardIT.class.getName());
    InjectorUtils.createTopic(pubsub, topic);

    PubsubIO.writeStrings()
        .to(topic)
        .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
        .withIdAttribute(projectId)
        .expand(input);

    options = TestPipeline.testingPipelineOptions().as(LeaderBoardOptions.class);
    options.as(GcpOptions.class).setProject(projectId);
    options.setDataset(outputDataset);
    options.setTopic(topic);
  }

  @Test
  public void testE2ELeaderBoard() throws Exception {
    LeaderBoard.runLeaderBoard(options);
  }

  @After
  public void cleanupTestEnvironment() throws Exception {
    bqClient.deleteDataset(projectId, outputDataset);
    pubsub.projects().topics().delete(topic);
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
