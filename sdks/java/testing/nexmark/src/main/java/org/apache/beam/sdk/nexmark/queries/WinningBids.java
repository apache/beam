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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.AuctionBid;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A transform to find the winning bid for each closed auction. In pseudo CQL syntax:
 *
 * <pre>{@code
 * SELECT Rstream(A.*, B.auction, B.bidder, MAX(B.price), B.dateTime)
 * FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 * WHERE A.id = B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
 * GROUP BY A.id
 * }</pre>
 *
 * <p>We will also check that the winning bid is above the auction reserve. Note that we ignore the
 * auction opening bid value since it has no impact on which bid eventually wins, if any.
 *
 * <p>Our implementation will use a custom windowing function in order to bring bids and auctions
 * together without requiring global state.
 */
public class WinningBids extends PTransform<PCollection<Event>, PCollection<AuctionBid>> {
  /** Windows for open auctions and bids. */
  private static class AuctionOrBidWindow extends IntervalWindow {
    /** Id of auction this window is for. */
    public final long auction;

    /**
     * True if this window represents an actual auction, and thus has a start/end time matching that
     * of the auction. False if this window represents a bid, and thus has an unbounded start/end
     * time.
     */
    public final boolean isAuctionWindow;

    /** For avro only. */
    private AuctionOrBidWindow() {
      super(TIMESTAMP_MIN_VALUE, TIMESTAMP_MAX_VALUE);
      auction = 0;
      isAuctionWindow = false;
    }

    private AuctionOrBidWindow(
        Instant start, Instant end, long auctionId, boolean isAuctionWindow) {
      super(start, end);
      this.auction = auctionId;
      this.isAuctionWindow = isAuctionWindow;
    }

    /** Return an auction window for {@code auction}. */
    public static AuctionOrBidWindow forAuction(Instant timestamp, Auction auction) {
      return new AuctionOrBidWindow(timestamp, new Instant(auction.expires), auction.id, true);
    }

    /**
     * Return a bid window for {@code bid}. It should later be merged into the corresponding auction
     * window. However, it is possible this bid is for an already expired auction, or for an auction
     * which the system has not yet seen. So we give the bid a bit of wiggle room in its interval.
     */
    public static AuctionOrBidWindow forBid(
        long expectedAuctionDurationMs, Instant timestamp, Bid bid) {
      // At this point we don't know which auctions are still valid, and the bid may
      // be for an auction which won't start until some unknown time in the future
      // (due to Generator.AUCTION_ID_LEAD in Generator.nextBid).
      // A real system would atomically reconcile bids and auctions by a separate mechanism.
      // If we give bids an unbounded window it is possible a bid for an auction which
      // has already expired would cause the system watermark to stall, since that window
      // would never be retired.
      // Instead, we will just give the bid a finite window which expires at
      // the upper bound of auctions assuming the auction starts at the same time as the bid,
      // and assuming the system is running at its lowest event rate (as per interEventDelayUs).
      return new AuctionOrBidWindow(
          timestamp, timestamp.plus(expectedAuctionDurationMs * 2), bid.auction, false);
    }

    /** Is this an auction window? */
    public boolean isAuctionWindow() {
      return isAuctionWindow;
    }

    @Override
    public String toString() {
      return String.format(
          "AuctionOrBidWindow{start:%s; end:%s; auction:%d; isAuctionWindow:%s}",
          start(), end(), auction, isAuctionWindow);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      AuctionOrBidWindow that = (AuctionOrBidWindow) o;
      return (isAuctionWindow == that.isAuctionWindow) && (auction == that.auction);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), isAuctionWindow, auction);
    }
  }

  /** Encodes an {@link AuctionOrBidWindow} as an {@link IntervalWindow} and an auction id long. */
  private static class AuctionOrBidWindowCoder extends CustomCoder<AuctionOrBidWindow> {
    private static final AuctionOrBidWindowCoder INSTANCE = new AuctionOrBidWindowCoder();
    private static final Coder<IntervalWindow> SUPER_CODER = IntervalWindow.getCoder();
    private static final Coder<Long> ID_CODER = VarLongCoder.of();
    private static final Coder<Integer> INT_CODER = VarIntCoder.of();

    @JsonCreator
    public static AuctionOrBidWindowCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(AuctionOrBidWindow window, OutputStream outStream)
        throws IOException, CoderException {
      SUPER_CODER.encode(window, outStream);
      ID_CODER.encode(window.auction, outStream);
      INT_CODER.encode(window.isAuctionWindow ? 1 : 0, outStream);
    }

    @Override
    public AuctionOrBidWindow decode(InputStream inStream) throws IOException, CoderException {
      IntervalWindow superWindow = SUPER_CODER.decode(inStream);
      long auction = ID_CODER.decode(inStream);
      boolean isAuctionWindow = INT_CODER.decode(inStream) != 0;
      return new AuctionOrBidWindow(
          superWindow.start(), superWindow.end(), auction, isAuctionWindow);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    @Override
    public Object structuralValue(AuctionOrBidWindow value) {
      return value;
    }
  }

  /** Assign events to auction windows and merges them intelligently. */
  private static class AuctionOrBidWindowFn extends WindowFn<Event, AuctionOrBidWindow> {
    /** Expected duration of auctions in ms. */
    private final long expectedAuctionDurationMs;

    public AuctionOrBidWindowFn(long expectedAuctionDurationMs) {
      this.expectedAuctionDurationMs = expectedAuctionDurationMs;
    }

    @Override
    public Collection<AuctionOrBidWindow> assignWindows(AssignContext c) {
      Event event = c.element();
      if (event.newAuction != null) {
        // Assign auctions to an auction window which expires at the auction's close.
        return Collections.singletonList(
            AuctionOrBidWindow.forAuction(c.timestamp(), event.newAuction));
      } else if (event.bid != null) {
        // Assign bids to a temporary bid window which will later be merged into the appropriate
        // auction window.
        return Collections.singletonList(
            AuctionOrBidWindow.forBid(expectedAuctionDurationMs, c.timestamp(), event.bid));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "%s can only assign windows to auctions and bids, but received %s",
                getClass().getSimpleName(), c.element()));
      }
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
      // Split and index the auction and bid windows by auction id.
      Map<Long, AuctionOrBidWindow> idToTrueAuctionWindow = new TreeMap<>();
      Map<Long, List<AuctionOrBidWindow>> idToBidAuctionWindows = new TreeMap<>();
      for (AuctionOrBidWindow window : c.windows()) {
        if (window.isAuctionWindow()) {
          idToTrueAuctionWindow.put(window.auction, window);
        } else {
          List<AuctionOrBidWindow> bidWindows =
              idToBidAuctionWindows.computeIfAbsent(window.auction, k -> new ArrayList<>());
          bidWindows.add(window);
        }
      }

      // Merge all 'bid' windows into their corresponding 'auction' window, provided the
      // auction has not expired.
      for (Map.Entry<Long, AuctionOrBidWindow> entry : idToTrueAuctionWindow.entrySet()) {
        long auction = entry.getKey();
        AuctionOrBidWindow auctionWindow = entry.getValue();
        List<AuctionOrBidWindow> bidWindows = idToBidAuctionWindows.get(auction);
        if (bidWindows != null) {
          List<AuctionOrBidWindow> toBeMerged = new ArrayList<>();
          for (AuctionOrBidWindow bidWindow : bidWindows) {
            if (bidWindow.start().isBefore(auctionWindow.end())) {
              toBeMerged.add(bidWindow);
            }
            // else: This bid window will remain until its expire time, at which point it
            // will expire without ever contributing to an output.
          }
          if (!toBeMerged.isEmpty()) {
            toBeMerged.add(auctionWindow);
            c.merge(toBeMerged, auctionWindow);
          }
        }
      }
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof AuctionOrBidWindowFn;
    }

    @Override
    public Coder<AuctionOrBidWindow> windowCoder() {
      return AuctionOrBidWindowCoder.of();
    }

    @Override
    public WindowMappingFn<AuctionOrBidWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException("AuctionWindowFn not supported for side inputs");
    }

    /**
     * Below we will GBK auctions and bids on their auction ids. Then we will reduce those per id to
     * emit {@code (auction, winning bid)} pairs for auctions which have expired with at least one
     * valid bid. We would like those output pairs to have a timestamp of the auction's expiry
     * (since that's the earliest we know for sure we have the correct winner). We would also like
     * to make that winning results are available to following stages at the auction's expiry.
     *
     * <p>Each result of the GBK will have a timestamp of the min of the result of this object's
     * assignOutputTime over all records which end up in one of its iterables. Thus we get the
     * desired behavior if we ignore each record's timestamp and always return the auction window's
     * 'maxTimestamp', which will correspond to the auction's expiry.
     *
     * <p>In contrast, if this object's assignOutputTime were to return 'inputTimestamp' (the usual
     * implementation), then each GBK record will take as its timestamp the minimum of the
     * timestamps of all bids and auctions within it, which will always be the auction's timestamp.
     * An auction which expires well into the future would thus hold up the watermark of the GBK
     * results until that auction expired. That in turn would hold up all winning pairs.
     */
    @Override
    public Instant getOutputTime(Instant inputTimestamp, AuctionOrBidWindow window) {
      return window.maxTimestamp();
    }
  }

  private final AuctionOrBidWindowFn auctionOrBidWindowFn;

  public WinningBids(String name, NexmarkConfiguration configuration) {
    super(name);
    // What's the expected auction time (when the system is running at the lowest event rate).
    long[] interEventDelayUs =
        configuration.rateShape.interEventDelayUs(
            configuration.firstEventRate, configuration.nextEventRate,
            configuration.rateUnit, configuration.numEventGenerators);
    long longestDelayUs = 0;
    for (long interEventDelayU : interEventDelayUs) {
      longestDelayUs = Math.max(longestDelayUs, interEventDelayU);
    }
    // Adjust for proportion of auction events amongst all events.
    longestDelayUs =
        (longestDelayUs * GeneratorConfig.PROPORTION_DENOMINATOR)
            / GeneratorConfig.AUCTION_PROPORTION;
    // Adjust for number of in-flight auctions.
    longestDelayUs = longestDelayUs * configuration.numInFlightAuctions;
    long expectedAuctionDurationMs = (longestDelayUs + 999) / 1000;
    NexmarkUtils.console("Expected auction duration is %d ms", expectedAuctionDurationMs);
    auctionOrBidWindowFn = new AuctionOrBidWindowFn(expectedAuctionDurationMs);
  }

  @Override
  public PCollection<AuctionBid> expand(PCollection<Event> events) {
    // Window auctions and bids into custom auction windows. New people events will be discarded.
    // This will allow us to bring bids and auctions together irrespective of how long
    // each auction is open for.
    events = events.apply("Window", Window.into(auctionOrBidWindowFn));

    // Key auctions by their id.
    PCollection<KV<Long, Auction>> auctionsById =
        events
            .apply(NexmarkQueryUtil.JUST_NEW_AUCTIONS)
            .apply("AuctionById:", NexmarkQueryUtil.AUCTION_BY_ID);

    // Key bids by their auction id.
    PCollection<KV<Long, Bid>> bidsByAuctionId =
        events
            .apply(NexmarkQueryUtil.JUST_BIDS)
            .apply("BidByAuction", NexmarkQueryUtil.BID_BY_AUCTION);

    // Find the highest price valid bid for each closed auction.
    return
    // Join auctions and bids.
    KeyedPCollectionTuple.of(NexmarkQueryUtil.AUCTION_TAG, auctionsById)
        .and(NexmarkQueryUtil.BID_TAG, bidsByAuctionId)
        .apply(CoGroupByKey.create())
        // Filter and select.
        .apply(
            name + ".Join",
            ParDo.of(
                new DoFn<KV<Long, CoGbkResult>, AuctionBid>() {
                  private final Counter noAuctionCounter = Metrics.counter(name, "noAuction");
                  private final Counter underReserveCounter = Metrics.counter(name, "underReserve");
                  private final Counter noValidBidsCounter = Metrics.counter(name, "noValidBids");

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    @Nullable
                    Auction auction =
                        c.element().getValue().getOnly(NexmarkQueryUtil.AUCTION_TAG, null);
                    if (auction == null) {
                      // We have bids without a matching auction. Give up.
                      noAuctionCounter.inc();
                      return;
                    }
                    // Find the current winning bid for auction.
                    // The earliest bid with the maximum price above the reserve wins.
                    Bid bestBid = null;
                    for (Bid bid : c.element().getValue().getAll(NexmarkQueryUtil.BID_TAG)) {
                      // Bids too late for their auction will have been
                      // filtered out by the window merge function.
                      checkState(bid.dateTime.compareTo(auction.expires) < 0);
                      if (bid.price < auction.reserve) {
                        // Bid price is below auction reserve.
                        underReserveCounter.inc();
                        continue;
                      }

                      if (bestBid == null
                          || Bid.PRICE_THEN_DESCENDING_TIME.compare(bid, bestBid) > 0) {
                        bestBid = bid;
                      }
                    }
                    if (bestBid == null) {
                      // We don't have any valid bids for auction.
                      noValidBidsCounter.inc();
                      return;
                    }
                    c.output(new AuctionBid(auction, bestBid));
                  }
                }));
  }

  @Override
  public int hashCode() {
    return Objects.hash(auctionOrBidWindowFn);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WinningBids that = (WinningBids) o;
    return auctionOrBidWindowFn.equals(that.auctionOrBidWindowFn);
  }
}
