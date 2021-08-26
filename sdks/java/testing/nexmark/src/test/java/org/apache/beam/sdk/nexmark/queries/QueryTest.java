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
package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test the various NEXMark queries yield results coherent with their models. */
@RunWith(JUnit4.class)
public class QueryTest {
  private static final NexmarkConfiguration CONFIG = NexmarkConfiguration.DEFAULT.copy();

  static {
    // careful, results of tests are linked to numEventGenerators because of timestamp generation
    CONFIG.numEventGenerators = 1;
    CONFIG.numEvents = 5000;
  }

  @Rule public TestPipeline p = TestPipeline.create();

  /** Test {@code query} matches {@code model}. */
  private <T extends KnownSize> void queryMatchesModel(
      String name,
      NexmarkQueryTransform<T> query,
      NexmarkQueryModel<T> model,
      boolean streamingMode) {
    NexmarkUtils.setupPipeline(NexmarkUtils.CoderStrategy.HAND, p);

    PCollection<Event> events =
        p.apply(
            name + ".Read",
            streamingMode
                ? NexmarkUtils.streamEventsSource(CONFIG)
                : NexmarkUtils.batchEventsSource(CONFIG));
    PCollection<TimestampedValue<T>> results =
        (PCollection<TimestampedValue<T>>) events.apply(new NexmarkQuery<>(CONFIG, query));
    PAssert.that(results).satisfies(model.assertionFor());
    PipelineResult result = p.run();
    result.waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void query0MatchesModelBatch() {
    queryMatchesModel("Query0TestBatch", new Query0(), new Query0Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query0MatchesModelStreaming() {
    queryMatchesModel("Query0TestStreaming", new Query0(), new Query0Model(CONFIG), true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query1MatchesModelBatch() {
    queryMatchesModel("Query1TestBatch", new Query1(CONFIG), new Query1Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query1MatchesModelStreaming() {
    queryMatchesModel("Query1TestStreaming", new Query1(CONFIG), new Query1Model(CONFIG), true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query2MatchesModelBatch() {
    queryMatchesModel("Query2TestBatch", new Query2(CONFIG), new Query2Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query2MatchesModelStreaming() {
    queryMatchesModel("Query2TestStreaming", new Query2(CONFIG), new Query2Model(CONFIG), true);
  }

  @Test
  @Category({NeedsRunner.class, UsesStatefulParDo.class, UsesTimersInParDo.class})
  public void query3MatchesModelBatch() {
    queryMatchesModel("Query3TestBatch", new Query3(CONFIG), new Query3Model(CONFIG), false);
  }

  @Test
  @Category({NeedsRunner.class, UsesStatefulParDo.class, UsesTimersInParDo.class})
  public void query3MatchesModelStreaming() {
    queryMatchesModel("Query3TestStreaming", new Query3(CONFIG), new Query3Model(CONFIG), true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query4MatchesModelBatch() {
    queryMatchesModel("Query4TestBatch", new Query4(CONFIG), new Query4Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query4MatchesModelStreaming() {
    queryMatchesModel("Query4TestStreaming", new Query4(CONFIG), new Query4Model(CONFIG), true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query5MatchesModelBatch() {
    queryMatchesModel("Query5TestBatch", new Query5(CONFIG), new Query5Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query5MatchesModelStreaming() {
    queryMatchesModel("Query5TestStreaming", new Query5(CONFIG), new Query5Model(CONFIG), true);
  }

  @Ignore("https://issues.apache.org/jira/browse/BEAM-3816")
  @Test
  @Category(NeedsRunner.class)
  public void query6MatchesModelBatch() {
    queryMatchesModel("Query6TestBatch", new Query6(CONFIG), new Query6Model(CONFIG), false);
  }

  @Ignore("https://issues.apache.org/jira/browse/BEAM-3816")
  @Test
  @Category(NeedsRunner.class)
  public void query6MatchesModelStreaming() {
    queryMatchesModel("Query6TestStreaming", new Query6(CONFIG), new Query6Model(CONFIG), true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query7MatchesModelBatch() {
    queryMatchesModel("Query7TestBatch", new Query7(CONFIG), new Query7Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query7MatchesModelStreaming() {
    queryMatchesModel("Query7TestStreaming", new Query7(CONFIG), new Query7Model(CONFIG), true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query8MatchesModelBatch() {
    queryMatchesModel("Query8TestBatch", new Query8(CONFIG), new Query8Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query8MatchesModelStreaming() {
    queryMatchesModel("Query8TestStreaming", new Query8(CONFIG), new Query8Model(CONFIG), true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query9MatchesModelBatch() {
    queryMatchesModel("Query9TestBatch", new Query9(CONFIG), new Query9Model(CONFIG), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void query9MatchesModelStreaming() {
    queryMatchesModel("Query9TestStreaming", new Query9(CONFIG), new Query9Model(CONFIG), true);
  }
}
