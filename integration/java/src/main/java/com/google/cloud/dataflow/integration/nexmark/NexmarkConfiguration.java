/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.integration.nexmark;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration controlling how a query is run. May be supplied by command line or
 * programmatically. Some properties override those supplied by {@link DataflowPipelineOptions} (eg
 * {@code isStreaming}). We try to capture everything which may influence the resulting
 * pipeline performance, as captured by {@link NexmarkPerf}.
 */
class NexmarkConfiguration implements Serializable {
  public static final NexmarkConfiguration DEFAULT = new NexmarkConfiguration();

  /** Which query to run, in [0,9]. */
  @JsonProperty
  public int query = 0;

  /** If true, emit query result as ERROR log entries. */
  @JsonProperty
  public boolean logResults = false;

  /**
   * If true, use {@link DataflowAssert} to assert that query results match a hand-written
   * model. Only works if {@link #numEvents} is small.
   */
  @JsonProperty
  public boolean assertCorrectness = false;

  /** Where events come from. */
  @JsonProperty
  public NexmarkUtils.SourceType sourceType = NexmarkUtils.SourceType.DIRECT;

  /** Where results go to. */
  @JsonProperty
  public NexmarkUtils.SinkType sinkType = NexmarkUtils.SinkType.DEVNULL;

  /**
   * Control whether pub/sub publishing is done in a stand-alone pipeline or is integrated
   * into the overall query pipeline.
   */
  @JsonProperty
  public NexmarkUtils.PubSubMode pubSubMode = NexmarkUtils.PubSubMode.COMBINED;

  /**
   * Number of events to generate. If zero, generate as many as possible without overflowing
   * internal counters etc.
   */
  @JsonProperty
  public long numEvents = 100000;

  /**
   * Number of events to generate at the special pre-load rate. This is useful for generating
   * a backlog of events on pub/sub before the main query begins.
   */
  @JsonProperty
  public long numPreloadEvents = 0;

  /**
   * Number of event generators to use. Each generates events in its own timeline.
   */
  @JsonProperty
  public int numEventGenerators = 100;

  /**
   * Shape of event qps curve.
   */
  @JsonProperty
  public NexmarkUtils.QpsShape qpsShape = NexmarkUtils.QpsShape.SINE;

  /**
   * Initial overall event qps.
   */
  @JsonProperty
  public int firstEventQps = 10000;

  /**
   * Next overall event qps.
   */
  @JsonProperty
  public int nextEventQps = 10000;

  /**
   * Overall period of qps shape, in seconds.
   */
  @JsonProperty
  public int qpsPeriodSec = 600;

  /**
   * Overall event qps while pre-loading. Typcially as large as possible for given pub/sub quota.
   */
  @JsonProperty
  public int preloadEventQps = 10000;

  /**
   * If true, and in streaming mode, generate events only when they are due according to their
   * timestamp.
   */
  @JsonProperty
  public boolean isRateLimited = false;

  /**
   * If true, use wallclock time as event time. Otherwise, use a deterministic
   * time in the past so that multiple runs will see exactly the same event streams
   * and should thus have exactly the same results.
   */
  @JsonProperty
  public boolean useWallclockEventTime = false;

  /** Average idealized size of a 'new person' event, in bytes. */
  @JsonProperty
  public int avgPersonByteSize = 200;

  /** Average idealized size of a 'new auction' event, in bytes. */
  @JsonProperty
  public int avgAuctionByteSize = 500;

  /** Average idealized size of a 'bid' event, in bytes. */
  @JsonProperty
  public int avgBidByteSize = 100;

  /** Ratio of bids to 'hot' auctions compared to all other auctions. */
  @JsonProperty
  public int hotAuctionRatio = 1;

  /** Ratio of auctions for 'hot' sellers compared to all other people. */
  @JsonProperty
  public int hotSellersRatio = 1;

  /** Ratio of bids for 'hot' bidders compared to all other people. */
  @JsonProperty
  public int hotBiddersRatio = 1;

  /** Window size, in seconds, for queries 3, 5, 7 and 8. */
  @JsonProperty
  public long windowSizeSec = 10;

  /** Sliding window period, in seconds, for query 5. */
  @JsonProperty
  public long windowPeriodSec = 5;

  /** Number of seconds to hold back events according to their reported timestamp. */
  @JsonProperty
  public long watermarkHoldbackSec = 0;

  /** Average number of auction which should be inflight at any time, per generator. */
  @JsonProperty
  public int numInFlightAuctions = 100;

  /** Maximum number of people to consider as active for placing auctions or bids. */
  @JsonProperty
  public int numActivePeople = 1000;

  /**
   * If true, don't run the actual query. Instead, calculate the distribution
   * of number of query results per (event time) minute according to the query model.
   */
  @JsonProperty
  public boolean justModelResultRate = false;

  /** Coder strategy to follow. */
  @JsonProperty
  public NexmarkUtils.CoderStrategy coderStrategy = NexmarkUtils.CoderStrategy.HAND;

  /**
   * Delay, in milliseconds, for each event. This will peg one core for this number
   * of milliseconds to simulate CPU-bound computation.
   */
  @JsonProperty
  public long cpuDelayMs = 0;

  /**
   * Extra data, in bytes, to save to persistent state for each event. This will force
   * i/o all the way to durable storage to simulate an I/O-bound computation.
   */
  @JsonProperty
  public long diskBusyBytes = 0;

  /**
   * Skip factor for query 2. We select bids for every {@code auctionSkip}'th auction.
   */
  @JsonProperty
  public int auctionSkip = 123;

  /**
   * Fanout for queries 4 (groups by category id), 5 and 7 (find a global maximum).
   */
  @JsonProperty
  public int fanout = 5;

  /**
   * Length of occasional delay to impose on events (in seconds).
   */
  @JsonProperty
  public long occasionalDelaySec = 0;

  /**
   * Probability that an event will be delayed by delayS.
   */
  @JsonProperty
  public double probDelayedEvent = 0.0;

  /**
   * Maximum size of each log file (in events). For Query10 only.
   */
  @JsonProperty
  public int maxLogEvents = 100_000;

  /**
   * If true, use pub/sub publish time instead of event time.
   */
  @JsonProperty
  public boolean usePubsubPublishTime = false;

  /**
   * Number of events in out-of-order groups. 1 implies no out-of-order events. 1000 implies
   * every 1000 events per generator are emitted in pseudo-random order.
   */
  @JsonProperty
  public long outOfOrderGroupSize = 1;

  /**
   * In debug mode, include Monitor & Snoop.
   */
  @JsonProperty
  public boolean debug = true;

  /**
   * Replace any properties of this configuration which have been supplied by the command line.
   * However *never replace* isStreaming since we can't tell if it was supplied by the command line
   * or merely has its default false value.
   */
  public void overrideFromOptions(Options options) {
    if (options.getQuery() != null) {
      query = options.getQuery();
    }
    if (options.getLogResults() != null) {
      logResults = options.getLogResults();
    }
    if (options.getAssertCorrectness() != null) {
      assertCorrectness = options.getAssertCorrectness();
    }
    if (options.getSourceType() != null) {
      sourceType = options.getSourceType();
    }
    if (options.getSinkType() != null) {
      sinkType = options.getSinkType();
    }
    if (options.getPubSubMode() != null) {
      pubSubMode = options.getPubSubMode();
    }
    if (options.getNumEvents() != null) {
      numEvents = options.getNumEvents();
    }
    if (options.getNumPreloadEvents() != null) {
      numPreloadEvents = options.getNumPreloadEvents();
    }
    if (options.getNumEventGenerators() != null) {
      numEventGenerators = options.getNumEventGenerators();
    }
    if (options.getQpsShape() != null) {
      qpsShape = options.getQpsShape();
    }
    if (options.getFirstEventQps() != null) {
      firstEventQps = options.getFirstEventQps();
    }
    if (options.getNextEventQps() != null) {
      nextEventQps = options.getNextEventQps();
    }
    if (options.getQpsPeriodSec() != null) {
      qpsPeriodSec = options.getQpsPeriodSec();
    }
    if (options.getPreloadEventQps() != null) {
      preloadEventQps = options.getPreloadEventQps();
    }
    if (options.getIsRateLimited() != null) {
      isRateLimited = options.getIsRateLimited();
    }
    if (options.getUseWallclockEventTime() != null) {
      useWallclockEventTime = options.getUseWallclockEventTime();
    }
    if (options.getAvgPersonByteSize() != null) {
      avgPersonByteSize = options.getAvgPersonByteSize();
    }
    if (options.getAvgAuctionByteSize() != null) {
      avgAuctionByteSize = options.getAvgAuctionByteSize();
    }
    if (options.getAvgBidByteSize() != null) {
      avgBidByteSize = options.getAvgBidByteSize();
    }
    if (options.getHotAuctionRatio() != null) {
      hotAuctionRatio = options.getHotAuctionRatio();
    }
    if (options.getHotSellersRatio() != null) {
      hotSellersRatio = options.getHotSellersRatio();
    }
    if (options.getHotBiddersRatio() != null) {
      hotBiddersRatio = options.getHotBiddersRatio();
    }
    if (options.getWindowSizeSec() != null) {
      windowSizeSec = options.getWindowSizeSec();
    }
    if (options.getWindowPeriodSec() != null) {
      windowPeriodSec = options.getWindowPeriodSec();
    }
    if (options.getWatermarkHoldbackSec() != null) {
      watermarkHoldbackSec = options.getWatermarkHoldbackSec();
    }
    if (options.getNumInFlightAuctions() != null) {
      numInFlightAuctions = options.getNumInFlightAuctions();
    }
    if (options.getNumActivePeople() != null) {
      numActivePeople = options.getNumActivePeople();
    }
    if (options.getJustModelResultRate() != null) {
      justModelResultRate = options.getJustModelResultRate();
    }
    if (options.getCoderStrategy() != null) {
      coderStrategy = options.getCoderStrategy();
    }
    if (options.getCpuDelayMs() != null) {
      cpuDelayMs = options.getCpuDelayMs();
    }
    if (options.getDiskBusyBytes() != null) {
      diskBusyBytes = options.getDiskBusyBytes();
    }
    if (options.getAuctionSkip() != null) {
      auctionSkip = options.getAuctionSkip();
    }
    if (options.getFanout() != null) {
      fanout = options.getFanout();
    }
    if (options.getOccasionalDelaySec() != null) {
      occasionalDelaySec = options.getOccasionalDelaySec();
    }
    if (options.getProbDelayedEvent() != null) {
      probDelayedEvent = options.getProbDelayedEvent();
    }
    if (options.getMaxLogEvents() != null) {
      maxLogEvents = options.getMaxLogEvents();
    }
    if (options.getUsePubsubPublishTime() != null) {
      usePubsubPublishTime = options.getUsePubsubPublishTime();
    }
    if (options.getOutOfOrderGroupSize() != null) {
      outOfOrderGroupSize = options.getOutOfOrderGroupSize();
    }
    if (options.getDebug() != null) {
      debug = options.getDebug();
    }
  }

  /**
   * Return clone of configuration with given label.
   */
  @Override
  public NexmarkConfiguration clone() {
    NexmarkConfiguration result = new NexmarkConfiguration();
    result.query = query;
    result.logResults = logResults;
    result.assertCorrectness = assertCorrectness;
    result.sourceType = sourceType;
    result.sinkType = sinkType;
    result.pubSubMode = pubSubMode;
    result.numEvents = numEvents;
    result.numPreloadEvents = numPreloadEvents;
    result.numEventGenerators = numEventGenerators;
    result.qpsShape = qpsShape;
    result.firstEventQps = firstEventQps;
    result.nextEventQps = nextEventQps;
    result.qpsPeriodSec = qpsPeriodSec;
    result.preloadEventQps = preloadEventQps;
    result.isRateLimited = isRateLimited;
    result.useWallclockEventTime = useWallclockEventTime;
    result.avgPersonByteSize = avgPersonByteSize;
    result.avgAuctionByteSize = avgAuctionByteSize;
    result.avgBidByteSize = avgBidByteSize;
    result.hotAuctionRatio = hotAuctionRatio;
    result.hotSellersRatio = hotSellersRatio;
    result.hotBiddersRatio = hotBiddersRatio;
    result.windowSizeSec = windowSizeSec;
    result.windowPeriodSec = windowPeriodSec;
    result.watermarkHoldbackSec = watermarkHoldbackSec;
    result.numInFlightAuctions = numInFlightAuctions;
    result.numActivePeople = numActivePeople;
    result.justModelResultRate = justModelResultRate;
    result.coderStrategy = coderStrategy;
    result.cpuDelayMs = cpuDelayMs;
    result.diskBusyBytes = diskBusyBytes;
    result.auctionSkip = auctionSkip;
    result.fanout = fanout;
    result.occasionalDelaySec = occasionalDelaySec;
    result.probDelayedEvent = probDelayedEvent;
    result.maxLogEvents = maxLogEvents;
    result.usePubsubPublishTime = usePubsubPublishTime;
    result.outOfOrderGroupSize = outOfOrderGroupSize;
    return result;
  }

  /**
   * Return short description of configuration (suitable for use in logging). We only render
   * the core fields plus those which do not have default values.
   */
  public String toShortString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("query:%d", query));
    if (sourceType != DEFAULT.sourceType) {
      sb.append(String.format("; sourceType:%s", sourceType));
    }
    if (sinkType != DEFAULT.sinkType) {
      sb.append(String.format("; sinkType:%s", sinkType));
    }
    if (pubSubMode != DEFAULT.pubSubMode) {
      sb.append(String.format("; pubSubMode:%s", pubSubMode));
    }
    if (numEvents != DEFAULT.numEvents) {
      sb.append(String.format("; numEvents:%d", numEvents));
    }
    if (numPreloadEvents != DEFAULT.numPreloadEvents) {
      sb.append(String.format("; numPreloadEvents:%d", numPreloadEvents));
    }
    if (numEventGenerators != DEFAULT.numEventGenerators) {
      sb.append(String.format("; numEventGenerators:%d", numEventGenerators));
    }
    if (qpsShape != DEFAULT.qpsShape) {
      sb.append(String.format("; qpsShare:%s", qpsShape));
    }
    if (firstEventQps != DEFAULT.firstEventQps || nextEventQps != DEFAULT.nextEventQps) {
      sb.append(String.format("; firstEventQps:%d", firstEventQps));
      sb.append(String.format("; nextEventQps:%d", nextEventQps));
    }
    if (qpsPeriodSec != DEFAULT.qpsPeriodSec) {
      sb.append(String.format("; qpsPeriodSec:%d", qpsPeriodSec));
    }
    if (preloadEventQps != DEFAULT.preloadEventQps) {
      sb.append(String.format("; preloadEventQps:%d", preloadEventQps));
    }
    if (isRateLimited != DEFAULT.isRateLimited) {
      sb.append(String.format("; isRateLimited:%s", isRateLimited));
    }
    if (useWallclockEventTime != DEFAULT.useWallclockEventTime) {
      sb.append(String.format("; useWallclockEventTime:%s", useWallclockEventTime));
    }
    if (avgPersonByteSize != DEFAULT.avgPersonByteSize) {
      sb.append(String.format("; avgPersonByteSize:%d", avgPersonByteSize));
    }
    if (avgAuctionByteSize != DEFAULT.avgAuctionByteSize) {
      sb.append(String.format("; avgAuctionByteSize:%d", avgAuctionByteSize));
    }
    if (avgBidByteSize != DEFAULT.avgBidByteSize) {
      sb.append(String.format("; avgBidByteSize:%d", avgBidByteSize));
    }
    if (hotAuctionRatio != DEFAULT.hotAuctionRatio) {
      sb.append(String.format("; hotAuctionRatio:%d", hotAuctionRatio));
    }
    if (hotSellersRatio != DEFAULT.hotSellersRatio) {
      sb.append(String.format("; hotSellersRatio:%d", hotSellersRatio));
    }
    if (hotBiddersRatio != DEFAULT.hotBiddersRatio) {
      sb.append(String.format("; hotBiddersRatio:%d", hotBiddersRatio));
    }
    if (windowSizeSec != DEFAULT.windowSizeSec) {
      sb.append(String.format("; windowSizeSec:%d", windowSizeSec));
    }
    if (windowPeriodSec != DEFAULT.windowPeriodSec) {
      sb.append(String.format("; windowPeriodSec:%d", windowPeriodSec));
    }
    if (watermarkHoldbackSec != DEFAULT.watermarkHoldbackSec) {
      sb.append(String.format("; watermarkHoldbackSec:%d", watermarkHoldbackSec));
    }
    if (numInFlightAuctions != DEFAULT.numInFlightAuctions) {
      sb.append(String.format("; numInFlightAuctions:%d", numInFlightAuctions));
    }
    if (numActivePeople != DEFAULT.numActivePeople) {
      sb.append(String.format("; numActivePeople:%d", numActivePeople));
    }
    if (justModelResultRate != DEFAULT.justModelResultRate) {
      sb.append(String.format("; justModelResutRate:%s", justModelResultRate));
    }
    if (coderStrategy != DEFAULT.coderStrategy) {
      sb.append(String.format("; coderStrategy:%s", coderStrategy));
    }
    if (cpuDelayMs != DEFAULT.cpuDelayMs) {
      sb.append(String.format("; cpuSlowdownMs:%d", cpuDelayMs));
    }
    if (diskBusyBytes != DEFAULT.diskBusyBytes) {
      sb.append(String.format("; diskBuysBytes:%d", diskBusyBytes));
    }
    if (auctionSkip != DEFAULT.auctionSkip) {
      sb.append(String.format("; auctionSkip:%d", auctionSkip));
    }
    if (fanout != DEFAULT.fanout) {
      sb.append(String.format("; fanout:%d", fanout));
    }
    if (occasionalDelaySec != DEFAULT.occasionalDelaySec) {
      sb.append(String.format("; occasionalDelaySec:%d", occasionalDelaySec));
    }
    if (probDelayedEvent != DEFAULT.probDelayedEvent) {
      sb.append(String.format("; probDelayedEvent:%f", probDelayedEvent));
    }
    if (maxLogEvents != DEFAULT.maxLogEvents) {
      sb.append(String.format("; maxLogEvents:%d", maxLogEvents));
    }
    if (usePubsubPublishTime != DEFAULT.usePubsubPublishTime) {
      sb.append(String.format("; usePubsubPublishTime:%s", usePubsubPublishTime));
    }
    if (outOfOrderGroupSize != DEFAULT.outOfOrderGroupSize) {
      sb.append(String.format("; outOfOrderGroupSize:%d", outOfOrderGroupSize));
    }
    return sb.toString();
  }

  /**
   * Return full description as a string.
   */
  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parse an object from {@code string}.
   *
   * @throws IOException
   */
  public static NexmarkConfiguration fromString(String string) throws IOException {
    return NexmarkUtils.MAPPER.readValue(string, NexmarkConfiguration.class);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, logResults, assertCorrectness, sourceType, sinkType, pubSubMode,
        numEvents, numPreloadEvents, numEventGenerators, qpsShape, firstEventQps, nextEventQps,
        qpsPeriodSec, preloadEventQps, isRateLimited, useWallclockEventTime, avgPersonByteSize,
        avgAuctionByteSize, avgBidByteSize, hotAuctionRatio, hotSellersRatio, hotBiddersRatio,
        windowSizeSec, windowPeriodSec, watermarkHoldbackSec, numInFlightAuctions, numActivePeople,
        justModelResultRate, coderStrategy, cpuDelayMs, diskBusyBytes, auctionSkip, fanout,
        occasionalDelaySec, probDelayedEvent, maxLogEvents, usePubsubPublishTime,
        outOfOrderGroupSize);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    NexmarkConfiguration other = (NexmarkConfiguration) obj;
    if (assertCorrectness != other.assertCorrectness) {
      return false;
    }
    if (auctionSkip != other.auctionSkip) {
      return false;
    }
    if (avgAuctionByteSize != other.avgAuctionByteSize) {
      return false;
    }
    if (avgBidByteSize != other.avgBidByteSize) {
      return false;
    }
    if (avgPersonByteSize != other.avgPersonByteSize) {
      return false;
    }
    if (coderStrategy != other.coderStrategy) {
      return false;
    }
    if (cpuDelayMs != other.cpuDelayMs) {
      return false;
    }
    if (diskBusyBytes != other.diskBusyBytes) {
      return false;
    }
    if (fanout != other.fanout) {
      return false;
    }
    if (firstEventQps != other.firstEventQps) {
      return false;
    }
    if (hotAuctionRatio != other.hotAuctionRatio) {
      return false;
    }
    if (hotBiddersRatio != other.hotBiddersRatio) {
      return false;
    }
    if (hotSellersRatio != other.hotSellersRatio) {
      return false;
    }
    if (isRateLimited != other.isRateLimited) {
      return false;
    }
    if (justModelResultRate != other.justModelResultRate) {
      return false;
    }
    if (logResults != other.logResults) {
      return false;
    }
    if (maxLogEvents != other.maxLogEvents) {
      return false;
    }
    if (nextEventQps != other.nextEventQps) {
      return false;
    }
    if (numEventGenerators != other.numEventGenerators) {
      return false;
    }
    if (numEvents != other.numEvents) {
      return false;
    }
    if (numInFlightAuctions != other.numInFlightAuctions) {
      return false;
    }
    if (numActivePeople != other.numActivePeople) {
      return false;
    }
    if (numPreloadEvents != other.numPreloadEvents) {
      return false;
    }
    if (occasionalDelaySec != other.occasionalDelaySec) {
      return false;
    }
    if (preloadEventQps != other.preloadEventQps) {
      return false;
    }
    if (Double.doubleToLongBits(probDelayedEvent)
        != Double.doubleToLongBits(other.probDelayedEvent)) {
      return false;
    }
    if (pubSubMode != other.pubSubMode) {
      return false;
    }
    if (qpsPeriodSec != other.qpsPeriodSec) {
      return false;
    }
    if (qpsShape != other.qpsShape) {
      return false;
    }
    if (query != other.query) {
      return false;
    }
    if (sinkType != other.sinkType) {
      return false;
    }
    if (sourceType != other.sourceType) {
      return false;
    }
    if (useWallclockEventTime != other.useWallclockEventTime) {
      return false;
    }
    if (watermarkHoldbackSec != other.watermarkHoldbackSec) {
      return false;
    }
    if (windowPeriodSec != other.windowPeriodSec) {
      return false;
    }
    if (windowSizeSec != other.windowSizeSec) {
      return false;
    }
    if (usePubsubPublishTime != other.usePubsubPublishTime) {
      return false;
    }
    if (outOfOrderGroupSize != other.outOfOrderGroupSize) {
      return false;
    }
    return true;
  }
}
