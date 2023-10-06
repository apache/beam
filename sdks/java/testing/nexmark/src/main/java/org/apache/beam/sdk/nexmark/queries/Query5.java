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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.AuctionCount;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.queries.Query5.TopCombineFn.Accum;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Query 5, 'Hot Items'. Which auctions have seen the most bids in the last hour (updated every
 * minute). In CQL syntax:
 *
 * <pre>{@code
 * SELECT Rstream(auction)
 * FROM (SELECT B1.auction, count(*) AS num
 *       FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
 *       GROUP BY B1.auction)
 * WHERE num >= ALL (SELECT count(*)
 *                   FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
 *                   GROUP BY B2.auction);
 * }</pre>
 *
 * <p>To make things a bit more dynamic and easier to test we use much shorter windows, and we'll
 * also preserve the bid counts.
 */
public class Query5 extends NexmarkQueryTransform<AuctionCount> {
  private final NexmarkConfiguration configuration;

  /** CombineFn that takes bidders with counts and keeps all bidders with the top count. */
  public static class TopCombineFn
      extends AccumulatingCombineFn<KV<Long, Long>, Accum, KV<Long, List<Long>>> {
    @Override
    public Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public Coder<Accum> getAccumulatorCoder(
        @NonNull CoderRegistry registry, @NonNull Coder<KV<Long, Long>> inputCoder) {
      JavaFieldSchema provider = new JavaFieldSchema();
      TypeDescriptor<Accum> typeDescriptor = new TypeDescriptor<Accum>() {};
      return SchemaCoder.of(
          provider.schemaFor(typeDescriptor),
          typeDescriptor,
          provider.toRowFunction(typeDescriptor),
          provider.fromRowFunction(typeDescriptor));
    }

    /** Accumulator that takes bidders with counts and keeps all bidders with the top count. */
    public static class Accum
        implements AccumulatingCombineFn.Accumulator<KV<Long, Long>, Accum, KV<Long, List<Long>>> {

      public ArrayList<Long> auctions = new ArrayList<>();
      public long count = 0;

      @Override
      public void addInput(KV<Long, Long> input) {
        if (input.getValue() > count) {
          count = input.getValue();
          auctions.clear();
          auctions.add(input.getKey());
        } else if (input.getValue() == count) {
          auctions.add(input.getKey());
        }
      }

      @Override
      public void mergeAccumulator(Accum other) {
        if (other.count > this.count) {
          this.count = other.count;
          this.auctions.clear();
          this.auctions.addAll(other.auctions);
        } else if (other.count == this.count) {
          this.auctions.addAll(other.auctions);
        }
      }

      @Override
      public KV<Long, List<Long>> extractOutput() {
        return KV.of(count, auctions);
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        Accum other = (Accum) o;
        return this.count == other.count && Iterables.elementsEqual(this.auctions, other.auctions);
      }

      @Override
      public int hashCode() {
        return Objects.hash(count, auctions);
      }
    }
  }

  public Query5(NexmarkConfiguration configuration) {
    super("Query5");
    this.configuration = configuration;
  }

  @Override
  public PCollection<AuctionCount> expand(PCollection<Event> events) {
    return events
        // Only want the bid events.
        .apply(NexmarkQueryUtil.JUST_BIDS)
        // Window the bids into sliding windows.
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(configuration.windowSizeSec))
                    .every(Duration.standardSeconds(configuration.windowPeriodSec))))
        // Project just the auction id.
        .apply("BidToAuction", NexmarkQueryUtil.BID_TO_AUCTION)

        // Count the number of bids per auction id.
        .apply(Count.perElement())

        // Keep only the auction ids with the most bids.
        .apply(
            Combine.globally(new TopCombineFn()).withoutDefaults().withFanout(configuration.fanout))

        // Project into result.
        .apply(
            name + ".Select",
            ParDo.of(
                new DoFn<KV<Long, List<Long>>, AuctionCount>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    long count = c.element().getKey();
                    for (long auction : c.element().getValue()) {
                      c.output(new AuctionCount(auction, count));
                    }
                  }
                }));
  }
}
