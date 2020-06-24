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
package org.apache.beam.sdk.nexmark.queries.sql;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

import java.util.Random;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.queries.BoundedSideInputJoinModel;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryModel;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test the various NEXMark queries yield results coherent with their models. */
@RunWith(Enclosed.class)
public class SqlBoundedSideInputJoinTest {

  private abstract static class SqlBoundedSideInputJoinTestCases {

    protected abstract SqlBoundedSideInputJoin getQuery(NexmarkConfiguration configuration);

    @Rule public TestPipeline p = TestPipeline.create();

    @Before
    public void setupPipeline() {
      NexmarkUtils.setupPipeline(NexmarkUtils.CoderStrategy.HAND, p);
    }

    /** Test {@code query} matches {@code model}. */
    private <T extends KnownSize> void queryMatchesModel(
        String name,
        NexmarkConfiguration config,
        NexmarkQueryTransform<T> query,
        NexmarkQueryModel<T> model,
        boolean streamingMode)
        throws Exception {

      ResourceId sideInputResourceId =
          FileSystems.matchNewResource(
              String.format(
                  "%s/JoinToFiles-%s", p.getOptions().getTempLocation(), new Random().nextInt()),
              false);
      config.sideInputUrl = sideInputResourceId.toString();

      try {
        PCollection<KV<Long, String>> sideInput = NexmarkUtils.prepareSideInput(p, config);
        query.setSideInput(sideInput);

        PCollection<Event> events =
            p.apply(
                name + ".Read",
                streamingMode
                    ? NexmarkUtils.streamEventsSource(config)
                    : NexmarkUtils.batchEventsSource(config));

        PCollection<TimestampedValue<T>> results =
            (PCollection<TimestampedValue<T>>) events.apply(new NexmarkQuery<>(config, query));
        PAssert.that(results).satisfies(model.assertionFor());
        PipelineResult result = p.run();
        result.waitUntilFinish();
      } finally {
        NexmarkUtils.cleanUpSideInput(config);
      }
    }

    /**
     * A smoke test that the count of input bids and outputs are the same, to help diagnose
     * flakiness in more complex tests.
     */
    @Test
    public void inputOutputSameEvents() throws Exception {
      NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();
      config.sideInputType = NexmarkUtils.SideInputType.DIRECT;
      config.numEventGenerators = 1;
      config.numEvents = 5000;
      config.sideInputRowCount = 10;
      config.sideInputNumShards = 3;
      PCollection<KV<Long, String>> sideInput = NexmarkUtils.prepareSideInput(p, config);

      try {
        PCollection<Event> input = p.apply(NexmarkUtils.batchEventsSource(config));
        PCollection<Bid> justBids = input.apply(NexmarkQueryUtil.JUST_BIDS);
        PCollection<Long> bidCount = justBids.apply("Count Bids", Count.globally());

        NexmarkQueryTransform<Bid> query = getQuery(config);
        query.setSideInput(sideInput);

        PCollection<TimestampedValue<Bid>> output =
            (PCollection<TimestampedValue<Bid>>) input.apply(new NexmarkQuery(config, query));
        PCollection<Long> outputCount = output.apply("Count outputs", Count.globally());

        PAssert.that(PCollectionList.of(bidCount).and(outputCount).apply(Flatten.pCollections()))
            .satisfies(
                counts -> {
                  assertThat(Iterables.size(counts), equalTo(2));
                  assertThat(Iterables.get(counts, 0), greaterThan(0L));
                  assertThat(Iterables.get(counts, 0), equalTo(Iterables.get(counts, 1)));
                  return null;
                });
        p.run();
      } finally {
        NexmarkUtils.cleanUpSideInput(config);
      }
    }

    @Test
    public void queryMatchesModelBatchDirect() throws Exception {
      NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();
      config.sideInputType = NexmarkUtils.SideInputType.DIRECT;
      config.numEventGenerators = 1;
      config.numEvents = 5000;
      config.sideInputRowCount = 10;
      config.sideInputNumShards = 3;

      queryMatchesModel(
          "SqlBoundedSideInputJoinTestBatch",
          config,
          getQuery(config),
          new BoundedSideInputJoinModel(config),
          false);
    }

    @Test
    public void queryMatchesModelStreamingDirect() throws Exception {
      NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();
      config.sideInputType = NexmarkUtils.SideInputType.DIRECT;
      config.numEventGenerators = 1;
      config.numEvents = 5000;
      config.sideInputRowCount = 10;
      config.sideInputNumShards = 3;
      queryMatchesModel(
          "SqlBoundedSideInputJoinTestStreaming",
          config,
          getQuery(config),
          new BoundedSideInputJoinModel(config),
          true);
    }

    @Test
    public void queryMatchesModelBatchCsv() throws Exception {
      NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();
      config.sideInputType = NexmarkUtils.SideInputType.CSV;
      config.numEventGenerators = 1;
      config.numEvents = 5000;
      config.sideInputRowCount = 10;
      config.sideInputNumShards = 3;

      queryMatchesModel(
          "SqlBoundedSideInputJoinTestBatch",
          config,
          getQuery(config),
          new BoundedSideInputJoinModel(config),
          false);
    }

    @Test
    public void queryMatchesModelStreamingCsv() throws Exception {
      NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();
      config.sideInputType = NexmarkUtils.SideInputType.CSV;
      config.numEventGenerators = 1;
      config.numEvents = 5000;
      config.sideInputRowCount = 10;
      config.sideInputNumShards = 3;
      queryMatchesModel(
          "SqlBoundedSideInputJoinTestStreaming",
          config,
          getQuery(config),
          new BoundedSideInputJoinModel(config),
          true);
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlBoundedSideInputJoinTestCalcite extends SqlBoundedSideInputJoinTestCases {
    @Override
    protected SqlBoundedSideInputJoin getQuery(NexmarkConfiguration configuration) {
      return SqlBoundedSideInputJoin.calciteSqlBoundedSideInputJoin(configuration);
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlBoundedSideInputJoinTestZetaSql extends SqlBoundedSideInputJoinTestCases {
    @Override
    protected SqlBoundedSideInputJoin getQuery(NexmarkConfiguration configuration) {
      return SqlBoundedSideInputJoin.zetaSqlBoundedSideInputJoin(configuration);
    }
  }
}
