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
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery1;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery2;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery3;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery5;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery7;
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
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test the various NEXMark queries yield results coherent with their models. */
@RunWith(Enclosed.class)
public class SqlQueryTest {
  private static final NexmarkConfiguration CONFIG = NexmarkConfiguration.DEFAULT.copy();

  static {
    // careful, results of tests are linked to numEventGenerators because of timestamp generation
    CONFIG.numEventGenerators = 1;
    CONFIG.numEvents = 5000;
  }

  private abstract static class SqlQueryTestCases {

    protected abstract SqlQuery1 getQuery1();

    protected abstract SqlQuery2 getQuery2(long skipFactor);

    protected abstract SqlQuery3 getQuery3(NexmarkConfiguration configuration);

    protected abstract SqlQuery5 getQuery5(NexmarkConfiguration configuration);

    protected abstract SqlQuery7 getQuery7(NexmarkConfiguration configuration);

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
    public void sqlQuery1MatchesModelBatch() {
      queryMatchesModel("SqlQuery1TestBatch", getQuery1(), new Query1Model(CONFIG), false);
    }

    @Test
    public void sqlQuery1MatchesModelStreaming() {
      queryMatchesModel("SqlQuery1TestStreaming", getQuery1(), new Query1Model(CONFIG), true);
    }

    @Test
    public void sqlQuery2MatchesModelBatch() {
      queryMatchesModel(
          "SqlQuery2TestBatch", getQuery2(CONFIG.auctionSkip), new Query2Model(CONFIG), false);
    }

    @Test
    public void sqlQuery2MatchesModelStreaming() {
      queryMatchesModel(
          "SqlQuery2TestStreaming", getQuery2(CONFIG.auctionSkip), new Query2Model(CONFIG), true);
    }

    @Test
    @Category({UsesStatefulParDo.class, UsesTimersInParDo.class})
    public void sqlQuery3MatchesModelBatch() {
      queryMatchesModel("SqlQuery3TestBatch", getQuery3(CONFIG), new Query3Model(CONFIG), false);
    }

    @Test
    @Category({UsesStatefulParDo.class, UsesTimersInParDo.class})
    public void sqlQuery3MatchesModelStreaming() {
      queryMatchesModel("SqlQuery3TestStreaming", getQuery3(CONFIG), new Query3Model(CONFIG), true);
    }

    @Test
    @Ignore("https://jira.apache.org/jira/browse/BEAM-7072")
    public void sqlQuery5MatchesModelBatch() {
      queryMatchesModel("SqlQuery5TestBatch", getQuery5(CONFIG), new Query5Model(CONFIG), false);
    }

    @Test
    @Ignore("https://jira.apache.org/jira/browse/BEAM-7072")
    public void sqlQuery5MatchesModelStreaming() {
      queryMatchesModel("SqlQuery5TestStreaming", getQuery5(CONFIG), new Query5Model(CONFIG), true);
    }

    @Test
    public void sqlQuery7MatchesModelBatch() {
      queryMatchesModel("SqlQuery7TestBatch", getQuery7(CONFIG), new Query7Model(CONFIG), false);
    }

    @Test
    public void sqlQuery7MatchesModelStreaming() {
      queryMatchesModel("SqlQuery7TestStreaming", getQuery7(CONFIG), new Query7Model(CONFIG), true);
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlQueryTestCalcite extends SqlQueryTestCases {
    @Override
    protected SqlQuery1 getQuery1() {
      return new SqlQuery1();
    }

    @Override
    protected SqlQuery2 getQuery2(long skipFactor) {
      return SqlQuery2.calciteSqlQuery2(skipFactor);
    }

    @Override
    protected SqlQuery3 getQuery3(NexmarkConfiguration configuration) {
      return SqlQuery3.calciteSqlQuery3(configuration);
    }

    @Override
    protected SqlQuery5 getQuery5(NexmarkConfiguration configuration) {
      return new SqlQuery5(configuration);
    }

    @Override
    protected SqlQuery7 getQuery7(NexmarkConfiguration configuration) {
      return new SqlQuery7(configuration);
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlQueryTestZetaSql extends SqlQueryTestCases {
    @Override
    protected SqlQuery1 getQuery1() {
      throw new UnsupportedOperationException("Query1 not implemented for ZetaSQL");
    }

    @Override
    protected SqlQuery2 getQuery2(long skipFactor) {
      return SqlQuery2.zetaSqlQuery2(skipFactor);
    }

    @Override
    protected SqlQuery3 getQuery3(NexmarkConfiguration configuration) {
      return SqlQuery3.zetaSqlQuery3(configuration);
    }

    @Override
    protected SqlQuery5 getQuery5(NexmarkConfiguration configuration) {
      throw new UnsupportedOperationException("Query5 is not implemented for ZetaSQL");
    }

    @Override
    protected SqlQuery7 getQuery7(NexmarkConfiguration configuration) {
      throw new UnsupportedOperationException("Query7 is not implemented for ZetaSQL");
    }

    @Override
    @Test
    @Ignore("Query1 is not implemented for ZetaSQL")
    public void sqlQuery1MatchesModelBatch() {
      throw new UnsupportedOperationException("Query1 is not implemented for ZetaSQL");
    }

    @Override
    @Test
    @Ignore("Query1 is not implemented for ZetaSQL")
    public void sqlQuery1MatchesModelStreaming() {
      throw new UnsupportedOperationException("Query1 is not implemented for ZetaSQL");
    }

    @Override
    @Test
    @Ignore("Query7 is not implemented for ZetaSQL")
    public void sqlQuery7MatchesModelBatch() {
      throw new UnsupportedOperationException("Query7 is not implemented for ZetaSQL");
    }

    @Override
    @Test
    @Ignore("Query7 is not implemented for ZetaSQL")
    public void sqlQuery7MatchesModelStreaming() {
      throw new UnsupportedOperationException("Query7 is not implemented for ZetaSQL");
    }
  }
}
