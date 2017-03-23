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
package org.apache.beam.integration.nexmark.queries;

import org.apache.beam.integration.nexmark.NexmarkConfiguration;
import org.apache.beam.integration.nexmark.NexmarkQuery;
import org.apache.beam.integration.nexmark.NexmarkQueryModel;
import org.apache.beam.integration.nexmark.NexmarkUtils;
import org.apache.beam.integration.nexmark.model.KnownSize;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test the various NEXMark queries yield results coherent with their models.
 */
@RunWith(JUnit4.class)
public class QueryTest {
  private static final NexmarkConfiguration CONFIG = NexmarkConfiguration.DEFAULT.clone();
  @Rule
  public TestPipeline p = TestPipeline.create();

  static {
    //careful, results of tests are linked to numEvents value
    CONFIG.numEventGenerators = 1;
    CONFIG.numEvents = 100;
  }

  /** Test {@code query} matches {@code model}. */
  private void queryMatchesModel(String name, NexmarkQuery query, NexmarkQueryModel model) {
    NexmarkUtils.setupPipeline(NexmarkUtils.CoderStrategy.HAND, p);
    PCollection<TimestampedValue<KnownSize>> results =
        p.apply(name + ".ReadBounded", NexmarkUtils.batchEventsSource(CONFIG)).apply(query);
    //TODO Ismael this should not be called explicitly
    results.setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
    PAssert.that(results).satisfies(model.assertionFor());
    PipelineResult result = p.run();
    result.waitUntilFinish();
  }

  @Test
  public void query0MatchesModel() {
    queryMatchesModel("Query0Test", new Query0(CONFIG), new Query0Model(CONFIG));
  }

  @Test
  public void query1MatchesModel() {
    queryMatchesModel("Query1Test", new Query1(CONFIG), new Query1Model(CONFIG));
  }

  @Test
  public void query2MatchesModel() {
    queryMatchesModel("Query2Test", new Query2(CONFIG), new Query2Model(CONFIG));
  }

  @Test
  public void query3MatchesModel() {
    queryMatchesModel("Query3Test", new Query3(CONFIG), new Query3Model(CONFIG));
  }

  @Test
  public void query4MatchesModel() {
    queryMatchesModel("Query4Test", new Query4(CONFIG), new Query4Model(CONFIG));
  }

  @Test
  public void query5MatchesModel() {
    queryMatchesModel("Query5Test", new Query5(CONFIG), new Query5Model(CONFIG));
  }

  @Test
  public void query6MatchesModel() {
    queryMatchesModel("Query6Test", new Query6(CONFIG), new Query6Model(CONFIG));
  }

  @Test
  public void query7MatchesModel() {
    queryMatchesModel("Query7Test", new Query7(CONFIG), new Query7Model(CONFIG));
  }

  @Test
  public void query8MatchesModel() {
    queryMatchesModel("Query8Test", new Query8(CONFIG), new Query8Model(CONFIG));
  }

  @Test
  public void query9MatchesModel() {
    queryMatchesModel("Query9Test", new Query9(CONFIG), new Query9Model(CONFIG));
  }
}
