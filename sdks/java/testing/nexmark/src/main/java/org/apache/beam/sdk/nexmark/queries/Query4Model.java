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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.AuctionBid;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.CategoryPrice;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;

/** A direct implementation of {@link Query4}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class Query4Model extends NexmarkQueryModel<CategoryPrice> implements Serializable {
  /** Simulator for query 4. */
  private class Simulator extends AbstractSimulator<AuctionBid, CategoryPrice> {
    /** The prices and categories for all winning bids in the last window size. */
    private final List<TimestampedValue<CategoryPrice>> winningPricesByCategory;

    /** Timestamp of last result (ms since epoch). */
    private Instant lastTimestamp;

    /** When oldest active window starts. */
    private Instant windowStart;

    /** The last seen result for each category. */
    private final Map<Long, TimestampedValue<CategoryPrice>> lastSeenResults;

    public Simulator(NexmarkConfiguration configuration) {
      super(new WinningBidsSimulator(configuration).results());
      winningPricesByCategory = new ArrayList<>();
      lastTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
      windowStart = NexmarkUtils.BEGINNING_OF_TIME;
      lastSeenResults = new TreeMap<>();
    }

    /**
     * Calculate the average bid price for each category for all winning bids which are strictly
     * before {@code end}.
     */
    private void averages(Instant end) {
      Map<Long, Long> counts = new TreeMap<>();
      Map<Long, Long> totals = new TreeMap<>();
      for (TimestampedValue<CategoryPrice> value : winningPricesByCategory) {
        if (!value.getTimestamp().isBefore(end)) {
          continue;
        }
        long category = value.getValue().category;
        long price = value.getValue().price;
        Long count = counts.get(category);
        if (count == null) {
          count = 1L;
        } else {
          count += 1;
        }
        counts.put(category, count);
        Long total = totals.get(category);
        if (total == null) {
          total = price;
        } else {
          total += price;
        }
        totals.put(category, total);
      }
      for (Map.Entry<Long, Long> entry : counts.entrySet()) {
        long category = entry.getKey();
        long count = entry.getValue();
        long total = totals.get(category);
        TimestampedValue<CategoryPrice> result =
            TimestampedValue.of(
                new CategoryPrice(category, Math.round((double) total / count), true),
                lastTimestamp);
        addIntermediateResult(result);
        lastSeenResults.put(category, result);
      }
    }

    /**
     * Calculate averages for any windows which can now be retired. Also prune entries which can no
     * longer contribute to any future window.
     */
    private void prune(Instant newWindowStart) {
      while (!newWindowStart.equals(windowStart)) {
        averages(windowStart.plus(Duration.standardSeconds(configuration.windowSizeSec)));
        windowStart = windowStart.plus(Duration.standardSeconds(configuration.windowPeriodSec));
        winningPricesByCategory.removeIf(
            categoryPriceTimestampedValue ->
                categoryPriceTimestampedValue.getTimestamp().isBefore(windowStart));
        if (winningPricesByCategory.isEmpty()) {
          windowStart = newWindowStart;
        }
      }
    }

    /** Capture the winning bid. */
    private void captureWinningBid(Auction auction, Bid bid, Instant timestamp) {
      winningPricesByCategory.add(
          TimestampedValue.of(new CategoryPrice(auction.category, bid.price, false), timestamp));
    }

    @Override
    protected void run() {
      TimestampedValue<AuctionBid> timestampedWinningBid = nextInput();
      if (timestampedWinningBid == null) {
        prune(NexmarkUtils.END_OF_TIME);
        for (TimestampedValue<CategoryPrice> result : lastSeenResults.values()) {
          addResult(result);
        }
        allDone();
        return;
      }
      lastTimestamp = timestampedWinningBid.getTimestamp();
      Instant newWindowStart =
          windowStart(
              Duration.standardSeconds(configuration.windowSizeSec),
              Duration.standardSeconds(configuration.windowPeriodSec),
              lastTimestamp);
      prune(newWindowStart);
      captureWinningBid(
          timestampedWinningBid.getValue().auction,
          timestampedWinningBid.getValue().bid,
          lastTimestamp);
    }
  }

  public Query4Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, CategoryPrice> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected Iterable<TimestampedValue<CategoryPrice>> relevantResults(
      Iterable<TimestampedValue<CategoryPrice>> results) {
    // Find the last (in processing time) reported average price for each category.
    Map<Long, TimestampedValue<CategoryPrice>> finalAverages = new TreeMap<>();
    for (TimestampedValue<CategoryPrice> obj : results) {
      Assert.assertTrue("have CategoryPrice", obj.getValue() instanceof CategoryPrice);
      CategoryPrice categoryPrice = (CategoryPrice) obj.getValue();
      if (categoryPrice.isLast) {
        finalAverages.put(
            categoryPrice.category, TimestampedValue.of(categoryPrice, obj.getTimestamp()));
      }
    }

    return finalAverages.values();
  }

  @Override
  protected Collection<String> toCollection(Iterator<TimestampedValue<CategoryPrice>> itr) {
    return toValue(itr);
  }
}
