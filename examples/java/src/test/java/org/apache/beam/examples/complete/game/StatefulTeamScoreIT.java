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

import com.google.api.services.pubsub.Pubsub;
import org.apache.beam.examples.complete.game.injector.InjectorUtils;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StatefulTeamScore}. */
@RunWith(JUnit4.class)
public class StatefulTeamScoreIT {
  private StatefulTeamScoreOptions options;
  private static Pubsub pubsub;
  private String projectId;
  private static String topic;
  private BigqueryClient bqClient;
  private final String timestamp = Long.toString(System.currentTimeMillis());
  private final String outputDataset = "stateful_team_score_e2e" + timestamp;

  public interface StatefulTeamScoreOptions
      extends TestPipelineOptions, StatefulTeamScore.Options {};

  @Before
  public void setupTestEnvironment() throws Exception {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    bqClient = new BigqueryClient("StatefulTeamScoreIT");
    bqClient.createNewDataset(projectId, outputDataset);

    pubsub = InjectorUtils.getClient();
    topic =
        InjectorUtils.getFullyQualifiedTopicName(
            projectId, StatefulTeamScoreIT.class.getName() + "_" + timestamp);
    InjectorUtils.createTopic(pubsub, topic);

    options = TestPipeline.testingPipelineOptions().as(StatefulTeamScoreOptions.class);
    options.as(GcpOptions.class).setProject(projectId);
    options.setDataset(outputDataset);
    options.setTopic(topic);
  }

  @Test
  public void testE2EStatefulTeamScoreOptions() throws Exception {
    StatefulTeamScore.runStatefulTeamScore(options);
  }

  @After
  public void cleanupTestEnvironment() throws Exception {
    bqClient.deleteDataset(projectId, outputDataset);
    pubsub.projects().topics().delete(topic);
  }
}
