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
package org.apache.beam.sdk.nexmark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Configuration controlling how a query is run. May be supplied by command line or
 * programmatically. We only capture properties which may influence the resulting pipeline
 * performance, as captured by {@link NexmarkPerf}.
 */
public class NexmarkConfiguration implements Serializable {
  public static final NexmarkConfiguration DEFAULT = new NexmarkConfiguration();

  /** If {@literal true}, include additional debugging and monitoring stats. */
  @JsonProperty public boolean debug = true;

  /** Which query to run, in [0,9]. */
  @JsonProperty public NexmarkQueryName query = null;

  /** Where events come from. */
  @JsonProperty public NexmarkUtils.SourceType sourceType = NexmarkUtils.SourceType.DIRECT;

  /** If provided, only generate events and write them to local file with this prefix. */
  @JsonProperty public String generateEventFilePathPrefix = null;

  /** Where results go to. */
  @JsonProperty public NexmarkUtils.SinkType sinkType = NexmarkUtils.SinkType.DEVNULL;

  /**
   * If false, the summary is only output to the console. If true the summary is output to the
   * console and it's content is written to bigquery tables according to {@link
   * NexmarkOptions#getResourceNameMode()}.
   */
  @JsonProperty public boolean exportSummaryToBigQuery = false;

  /**
   * Control whether pub/sub publishing is done in a stand-alone pipeline or is integrated into the
   * overall query pipeline.
   */
  @JsonProperty public NexmarkUtils.PubSubMode pubSubMode = NexmarkUtils.PubSubMode.COMBINED;

  /** Control the serialization/deserialization method from event object to pubsub messages. */
  @JsonProperty
  public NexmarkUtils.PubsubMessageSerializationMethod pubsubMessageSerializationMethod =
      NexmarkUtils.PubsubMessageSerializationMethod.CODER;

  /** The type of side input to use. */
  @JsonProperty public NexmarkUtils.SideInputType sideInputType = NexmarkUtils.SideInputType.DIRECT;

  /** Specify the number of rows to write to the side input. */
  @JsonProperty public int sideInputRowCount = 500;

  /** Specify the number of shards to write to the side input. */
  @JsonProperty public int sideInputNumShards = 3;

  /**
   * Specify a prefix URL for side input files, which will be created for use queries that join the
   * stream to static enrichment data.
   */
  @JsonProperty public String sideInputUrl = null;

  /** Gap for the session in {@link org.apache.beam.sdk.nexmark.queries.SessionSideInputJoin}. */
  @JsonProperty public Duration sessionGap = Duration.standardMinutes(10);

  /**
   * Number of events to generate. If zero, generate as many as possible without overflowing
   * internal counters etc.
   */
  @JsonProperty public long numEvents = 100000;

  /** Number of event generators to use. Each generates events in its own timeline. */
  @JsonProperty public int numEventGenerators = 100;

  /** Shape of event rate curve. */
  @JsonProperty public NexmarkUtils.RateShape rateShape = NexmarkUtils.RateShape.SINE;

  /** Initial overall event rate (in {@link #rateUnit}). */
  @JsonProperty public int firstEventRate = 10000;

  /** Next overall event rate (in {@link #rateUnit}). */
  @JsonProperty public int nextEventRate = 10000;

  /** Unit for rates. */
  @JsonProperty public NexmarkUtils.RateUnit rateUnit = NexmarkUtils.RateUnit.PER_SECOND;

  /** Overall period of rate shape, in seconds. */
  @JsonProperty public int ratePeriodSec = 600;

  /**
   * Time in seconds to preload the subscription with data, at the initial input rate of the
   * pipeline.
   */
  @JsonProperty public int preloadSeconds = 0;

  /** Timeout for stream pipelines to stop in seconds. */
  @JsonProperty public int streamTimeout = 240;

  /**
   * If true, and in streaming mode, generate events only when they are due according to their
   * timestamp.
   */
  @JsonProperty public boolean isRateLimited = false;

  /**
   * If true, use wallclock time as event time. Otherwise, use a deterministic time in the past so
   * that multiple runs will see exactly the same event streams and should thus have exactly the
   * same results.
   */
  @JsonProperty public boolean useWallclockEventTime = false;

  /** Average idealized size of a 'new person' event, in bytes. */
  @JsonProperty public int avgPersonByteSize = 200;

  /** Average idealized size of a 'new auction' event, in bytes. */
  @JsonProperty public int avgAuctionByteSize = 500;

  /** Average idealized size of a 'bid' event, in bytes. */
  @JsonProperty public int avgBidByteSize = 100;

  /** Ratio of bids to 'hot' auctions compared to all other auctions. */
  @JsonProperty public int hotAuctionRatio = 2;

  /** Ratio of auctions for 'hot' sellers compared to all other people. */
  @JsonProperty public int hotSellersRatio = 4;

  /** Ratio of bids for 'hot' bidders compared to all other people. */
  @JsonProperty public int hotBiddersRatio = 4;

  /** Window size, in seconds, for queries 3, 5, 7 and 8. */
  @JsonProperty public long windowSizeSec = 10;

  /** Sliding window period, in seconds, for query 5. */
  @JsonProperty public long windowPeriodSec = 5;

  /** Number of seconds to hold back events according to their reported timestamp. */
  @JsonProperty public long watermarkHoldbackSec = 0;

  /** Average number of auction which should be inflight at any time, per generator. */
  @JsonProperty public int numInFlightAuctions = 100;

  /** Maximum number of people to consider as active for placing auctions or bids. */
  @JsonProperty public int numActivePeople = 1000;

  /** Coder strategy to follow. */
  @JsonProperty public NexmarkUtils.CoderStrategy coderStrategy = NexmarkUtils.CoderStrategy.HAND;

  /**
   * Delay, in milliseconds, for each event. This will peg one core for this number of milliseconds
   * to simulate CPU-bound computation.
   */
  @JsonProperty public long cpuDelayMs = 0;

  /**
   * Extra data, in bytes, to save to persistent state for each event. This will force i/o all the
   * way to durable storage to simulate an I/O-bound computation.
   */
  @JsonProperty public long diskBusyBytes = 0;

  /** Skip factor for query 2. We select bids for every {@code auctionSkip}'th auction. */
  @JsonProperty public int auctionSkip = 123;

  /** Fanout for queries 4 (groups by category id), 5 and 7 (find a global maximum). */
  @JsonProperty public int fanout = 5;

  /**
   * Maximum waiting time to clean personState in query3 (ie maximum waiting of the auctions related
   * to person in state in seconds in event time).
   */
  @JsonProperty public int maxAuctionsWaitingTime = 600;

  /** Length of occasional delay to impose on events (in seconds). */
  @JsonProperty public long occasionalDelaySec = 3;

  /** Probability that an event will be delayed by delayS. */
  @JsonProperty public double probDelayedEvent = 0.1;

  /** Maximum size of each log file (in events). For Query10 only. */
  @JsonProperty public int maxLogEvents = 100_000;

  /** If true, use pub/sub publish time instead of event time. */
  @JsonProperty public boolean usePubsubPublishTime = false;

  /**
   * Number of events in out-of-order groups. 1 implies no out-of-order events. 1000 implies every
   * 1000 events per generator are emitted in pseudo-random order.
   */
  @JsonProperty public long outOfOrderGroupSize = 1;

  /** Replace any properties of this configuration which have been supplied by the command line. */
  public void overrideFromOptions(NexmarkOptions options) {
    if (options.getDebug() != null) {
      debug = options.getDebug();
    }
    if (options.getQuery() != null) {
      query = NexmarkQueryName.fromId(options.getQuery());
    }
    if (options.getSourceType() != null) {
      sourceType = options.getSourceType();
    }
    if (options.getGenerateEventFilePathPrefix() != null) {
      generateEventFilePathPrefix = options.getGenerateEventFilePathPrefix();
    }

    if (options.getSinkType() != null) {
      sinkType = options.getSinkType();
    }
    if (options.getExportSummaryToBigQuery() != null) {
      exportSummaryToBigQuery = options.getExportSummaryToBigQuery();
    }
    if (options.getPubSubMode() != null) {
      pubSubMode = options.getPubSubMode();
    }
    if (options.getPubsubMessageSerializationMethod() != null) {
      pubsubMessageSerializationMethod = options.getPubsubMessageSerializationMethod();
    }
    if (options.getNumEvents() != null) {
      numEvents = options.getNumEvents();
    }
    if (options.getNumEventGenerators() != null) {
      numEventGenerators = options.getNumEventGenerators();
    }
    if (options.getRateShape() != null) {
      rateShape = options.getRateShape();
    }
    if (options.getFirstEventRate() != null) {
      firstEventRate = options.getFirstEventRate();
    }
    if (options.getNextEventRate() != null) {
      nextEventRate = options.getNextEventRate();
    }
    if (options.getRateUnit() != null) {
      rateUnit = options.getRateUnit();
    }
    if (options.getRatePeriodSec() != null) {
      ratePeriodSec = options.getRatePeriodSec();
    }
    if (options.getPreloadSeconds() != null) {
      preloadSeconds = options.getPreloadSeconds();
    }
    if (options.getStreamTimeout() != null) {
      streamTimeout = options.getStreamTimeout();
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
    if (options.getMaxAuctionsWaitingTime() != null) {
      fanout = options.getMaxAuctionsWaitingTime();
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
  }

  /** Return copy of configuration with given label. */
  public NexmarkConfiguration copy() {
    NexmarkConfiguration result;
    result = new NexmarkConfiguration();
    result.debug = debug;
    result.query = query;
    result.sourceType = sourceType;
    result.sinkType = sinkType;
    result.exportSummaryToBigQuery = exportSummaryToBigQuery;
    result.pubSubMode = pubSubMode;
    result.pubsubMessageSerializationMethod = pubsubMessageSerializationMethod;
    result.numEvents = numEvents;
    result.numEventGenerators = numEventGenerators;
    result.rateShape = rateShape;
    result.firstEventRate = firstEventRate;
    result.nextEventRate = nextEventRate;
    result.rateUnit = rateUnit;
    result.ratePeriodSec = ratePeriodSec;
    result.preloadSeconds = preloadSeconds;
    result.streamTimeout = streamTimeout;
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
    result.coderStrategy = coderStrategy;
    result.cpuDelayMs = cpuDelayMs;
    result.diskBusyBytes = diskBusyBytes;
    result.auctionSkip = auctionSkip;
    result.fanout = fanout;
    result.maxAuctionsWaitingTime = maxAuctionsWaitingTime;
    result.occasionalDelaySec = occasionalDelaySec;
    result.probDelayedEvent = probDelayedEvent;
    result.maxLogEvents = maxLogEvents;
    result.usePubsubPublishTime = usePubsubPublishTime;
    result.outOfOrderGroupSize = outOfOrderGroupSize;
    return result;
  }

  /**
   * Return short description of configuration (suitable for use in logging). We only render the
   * core fields plus those which do not have default values.
   */
  public String toShortString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("query:%s", query));
    if (debug != DEFAULT.debug) {
      sb.append(String.format("; debug:%s", debug));
    }
    if (sourceType != DEFAULT.sourceType) {
      sb.append(String.format("; sourceType:%s", sourceType));
    }
    if (sinkType != DEFAULT.sinkType) {
      sb.append(String.format("; sinkType:%s", sinkType));
    }
    if (exportSummaryToBigQuery != DEFAULT.exportSummaryToBigQuery) {
      sb.append(String.format("; exportSummaryToBigQuery:%s", exportSummaryToBigQuery));
    }
    if (pubSubMode != DEFAULT.pubSubMode) {
      sb.append(String.format("; pubSubMode:%s", pubSubMode));
    }
    if (pubsubMessageSerializationMethod != DEFAULT.pubsubMessageSerializationMethod) {
      sb.append(
          String.format("; pubsubMessageSerializationMethod:%s", pubsubMessageSerializationMethod));
    }
    if (numEvents != DEFAULT.numEvents) {
      sb.append(String.format("; numEvents:%d", numEvents));
    }
    if (numEventGenerators != DEFAULT.numEventGenerators) {
      sb.append(String.format("; numEventGenerators:%d", numEventGenerators));
    }
    if (rateShape != DEFAULT.rateShape) {
      sb.append(String.format("; rateShape:%s", rateShape));
    }
    if (firstEventRate != DEFAULT.firstEventRate || nextEventRate != DEFAULT.nextEventRate) {
      sb.append(String.format("; firstEventRate:%d", firstEventRate));
      sb.append(String.format("; nextEventRate:%d", nextEventRate));
    }
    if (rateUnit != DEFAULT.rateUnit) {
      sb.append(String.format("; rateUnit:%s", rateUnit));
    }
    if (ratePeriodSec != DEFAULT.ratePeriodSec) {
      sb.append(String.format("; ratePeriodSec:%d", ratePeriodSec));
    }
    if (preloadSeconds != DEFAULT.preloadSeconds) {
      sb.append(String.format("; preloadSeconds:%d", preloadSeconds));
    }
    if (streamTimeout != DEFAULT.streamTimeout) {
      sb.append(String.format("; streamTimeout:%d", streamTimeout));
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
    if (maxAuctionsWaitingTime != DEFAULT.maxAuctionsWaitingTime) {
      sb.append(String.format("; maxAuctionsWaitingTime:%d", fanout));
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

  /** Return full description as a string. */
  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Parse an object from {@code string}. */
  public static NexmarkConfiguration fromString(String string) {
    try {
      return NexmarkUtils.MAPPER.readValue(string, NexmarkConfiguration.class);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse nexmark configuration: ", e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        debug,
        query,
        sourceType,
        sinkType,
        exportSummaryToBigQuery,
        pubSubMode,
        pubsubMessageSerializationMethod,
        numEvents,
        numEventGenerators,
        rateShape,
        firstEventRate,
        nextEventRate,
        rateUnit,
        ratePeriodSec,
        preloadSeconds,
        streamTimeout,
        isRateLimited,
        useWallclockEventTime,
        avgPersonByteSize,
        avgAuctionByteSize,
        avgBidByteSize,
        hotAuctionRatio,
        hotSellersRatio,
        hotBiddersRatio,
        windowSizeSec,
        windowPeriodSec,
        watermarkHoldbackSec,
        numInFlightAuctions,
        numActivePeople,
        coderStrategy,
        cpuDelayMs,
        diskBusyBytes,
        auctionSkip,
        fanout,
        maxAuctionsWaitingTime,
        occasionalDelaySec,
        probDelayedEvent,
        maxLogEvents,
        usePubsubPublishTime,
        outOfOrderGroupSize);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
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
    if (debug != other.debug) {
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
    if (maxAuctionsWaitingTime != other.maxAuctionsWaitingTime) {
      return false;
    }
    if (firstEventRate != other.firstEventRate) {
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
    if (maxLogEvents != other.maxLogEvents) {
      return false;
    }
    if (nextEventRate != other.nextEventRate) {
      return false;
    }
    if (rateUnit != other.rateUnit) {
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
    if (occasionalDelaySec != other.occasionalDelaySec) {
      return false;
    }
    if (preloadSeconds != other.preloadSeconds) {
      return false;
    }
    if (streamTimeout != other.streamTimeout) {
      return false;
    }
    if (Double.doubleToLongBits(probDelayedEvent)
        != Double.doubleToLongBits(other.probDelayedEvent)) {
      return false;
    }
    if (pubSubMode != other.pubSubMode) {
      return false;
    }
    if (pubsubMessageSerializationMethod != other.pubsubMessageSerializationMethod) {
      return false;
    }
    if (ratePeriodSec != other.ratePeriodSec) {
      return false;
    }
    if (rateShape != other.rateShape) {
      return false;
    }
    if (query != other.query) {
      return false;
    }
    if (sinkType != other.sinkType) {
      return false;
    }
    if (exportSummaryToBigQuery != other.exportSummaryToBigQuery) {
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
