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
package org.apache.beam.sdk.nexmark.sources;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

/**
 * A generator for synthetic events. We try to make the data vaguely reasonable. We also ensure
 * most primary key/foreign key relations are correct. Eg: a {@link Bid} event will usually have
 * valid auction and bidder ids which can be joined to already-generated Auction and Person events.
 *
 * <p>To help with testing, we generate timestamps relative to a given {@code baseTime}. Each new
 * event is given a timestamp advanced from the previous timestamp by {@code interEventDelayUs}
 * (in microseconds). The event stream is thus fully deterministic and does not depend on
 * wallclock time.
 *
 * <p>This class implements {@link org.apache.beam.sdk.io.UnboundedSource.CheckpointMark}
 * so that we can resume generating events from a saved snapshot.
 */
public class Generator implements Iterator<TimestampedValue<Event>>, Serializable {
  /**
   * Keep the number of categories small so the example queries will find results even with
   * a small batch of events.
   */
  private static final int NUM_CATEGORIES = 5;

  /** Smallest random string size. */
  private static final int MIN_STRING_LENGTH = 3;

  /**
   * Keep the number of states small so that the example queries will find results even with
   * a small batch of events.
   */
  private static final List<String> US_STATES = Arrays.asList(("AZ,CA,ID,OR,WA,WY").split(","));

  private static final List<String> US_CITIES =
      Arrays.asList(
          ("Phoenix,Los Angeles,San Francisco,Boise,Portland,Bend,Redmond,Seattle,Kent,Cheyenne")
              .split(","));

  private static final List<String> FIRST_NAMES =
      Arrays.asList(("Peter,Paul,Luke,John,Saul,Vicky,Kate,Julie,Sarah,Deiter,Walter").split(","));

  private static final List<String> LAST_NAMES =
      Arrays.asList(("Shultz,Abrams,Spencer,White,Bartels,Walton,Smith,Jones,Noris").split(","));

  /**
   * Number of yet-to-be-created people and auction ids allowed.
   */
  private static final int PERSON_ID_LEAD = 10;
  private static final int AUCTION_ID_LEAD = 10;

  /**
   * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1
   * over these values.
   */
  private static final int HOT_AUCTION_RATIO = 100;
  private static final int HOT_SELLER_RATIO = 100;
  private static final int HOT_BIDDER_RATIO = 100;

  /**
   * Just enough state to be able to restore a generator back to where it was checkpointed.
   */
  public static class Checkpoint implements UnboundedSource.CheckpointMark {
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();

    /** Coder for this class. */
    public static final Coder<Checkpoint> CODER_INSTANCE =
        new CustomCoder<Checkpoint>() {
          @Override public void encode(Checkpoint value, OutputStream outStream)
          throws CoderException, IOException {
            LONG_CODER.encode(value.numEvents, outStream);
            LONG_CODER.encode(value.wallclockBaseTime, outStream);
          }

          @Override
          public Checkpoint decode(InputStream inStream)
              throws CoderException, IOException {
            long numEvents = LONG_CODER.decode(inStream);
            long wallclockBaseTime = LONG_CODER.decode(inStream);
            return new Checkpoint(numEvents, wallclockBaseTime);
          }
          @Override public void verifyDeterministic() throws NonDeterministicException {}
        };

    private final long numEvents;
    private final long wallclockBaseTime;

    private Checkpoint(long numEvents, long wallclockBaseTime) {
      this.numEvents = numEvents;
      this.wallclockBaseTime = wallclockBaseTime;
    }

    public Generator toGenerator(GeneratorConfig config) {
      return new Generator(config, numEvents, wallclockBaseTime);
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      // Nothing to finalize.
    }

    @Override
    public String toString() {
      return String.format("Generator.Checkpoint{numEvents:%d;wallclockBaseTime:%d}",
          numEvents, wallclockBaseTime);
    }
  }

  /**
   * The next event and its various timestamps. Ordered by increasing wallclock timestamp, then
   * (arbitrary but stable) event hash order.
   */
  public static class NextEvent implements Comparable<NextEvent> {
    /** When, in wallclock time, should this event be emitted? */
    public final long wallclockTimestamp;

    /** When, in event time, should this event be considered to have occured? */
    public final long eventTimestamp;

    /** The event itself. */
    public final Event event;

    /** The minimum of this and all future event timestamps. */
    public final long watermark;

    public NextEvent(long wallclockTimestamp, long eventTimestamp, Event event, long watermark) {
      this.wallclockTimestamp = wallclockTimestamp;
      this.eventTimestamp = eventTimestamp;
      this.event = event;
      this.watermark = watermark;
    }

    /**
     * Return a deep copy of next event with delay added to wallclock timestamp and
     * event annotate as 'LATE'.
     */
    public NextEvent withDelay(long delayMs) {
      return new NextEvent(
          wallclockTimestamp + delayMs, eventTimestamp, event.withAnnotation("LATE"), watermark);
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      NextEvent nextEvent = (NextEvent) o;

      return (wallclockTimestamp == nextEvent.wallclockTimestamp
          && eventTimestamp == nextEvent.eventTimestamp
          && watermark == nextEvent.watermark
          && event.equals(nextEvent.event));
    }

    @Override public int hashCode() {
      return Objects.hash(wallclockTimestamp, eventTimestamp, watermark, event);
    }

    @Override
    public int compareTo(NextEvent other) {
      int i = Long.compare(wallclockTimestamp, other.wallclockTimestamp);
      if (i != 0) {
        return i;
      }
      return Integer.compare(event.hashCode(), other.event.hashCode());
    }
  }

  /**
   * Configuration to generate events against. Note that it may be replaced by a call to
   * {@link #splitAtEventId}.
   */
  private GeneratorConfig config;

  /** Number of events generated by this generator. */
  private long numEvents;

  /**
   * Wallclock time at which we emitted the first event (ms since epoch). Initially -1.
   */
  private long wallclockBaseTime;

  private Generator(GeneratorConfig config, long numEvents, long wallclockBaseTime) {
    checkNotNull(config);
    this.config = config;
    this.numEvents = numEvents;
    this.wallclockBaseTime = wallclockBaseTime;
  }

  /**
   * Create a fresh generator according to {@code config}.
   */
  public Generator(GeneratorConfig config) {
    this(config, 0, -1);
  }

  /**
   * Return a checkpoint for the current generator.
   */
  public Checkpoint toCheckpoint() {
    return new Checkpoint(numEvents, wallclockBaseTime);
  }

  /**
   * Return a deep copy of this generator.
   */
  public Generator copy() {
    checkNotNull(config);
    Generator result = new Generator(config, numEvents, wallclockBaseTime);
    return result;
  }

  /**
   * Return the current config for this generator. Note that configs may be replaced by {@link
   * #splitAtEventId}.
   */
  public GeneratorConfig getCurrentConfig() {
    return config;
  }

  /**
   * Mutate this generator so that it will only generate events up to but not including
   * {@code eventId}. Return a config to represent the events this generator will no longer yield.
   * The generators will run in on a serial timeline.
   */
  public GeneratorConfig splitAtEventId(long eventId) {
    long newMaxEvents = eventId - (config.firstEventId + config.firstEventNumber);
    GeneratorConfig remainConfig = config.copyWith(config.firstEventId,
        config.maxEvents - newMaxEvents, config.firstEventNumber + newMaxEvents);
    config = config.copyWith(config.firstEventId, newMaxEvents, config.firstEventNumber);
    return remainConfig;
  }

  /**
   * Return the next 'event id'. Though events don't have ids we can simulate them to
   * help with bookkeeping.
   */
  public long getNextEventId() {
    return config.firstEventId + config.nextAdjustedEventNumber(numEvents);
  }

  /**
   * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
   * due to generate a person.
   */
  private long lastBase0PersonId() {
    long eventId = getNextEventId();
    long epoch = eventId / GeneratorConfig.PROPORTION_DENOMINATOR;
    long offset = eventId % GeneratorConfig.PROPORTION_DENOMINATOR;
    if (offset >= GeneratorConfig.PERSON_PROPORTION) {
      // About to generate an auction or bid.
      // Go back to the last person generated in this epoch.
      offset = GeneratorConfig.PERSON_PROPORTION - 1;
    }
    // About to generate a person.
    return epoch * GeneratorConfig.PERSON_PROPORTION + offset;
  }

  /**
   * Return the last valid auction id (ignoring FIRST_AUCTION_ID). Will be the current auction id if
   * due to generate an auction.
   */
  private long lastBase0AuctionId() {
    long eventId = getNextEventId();
    long epoch = eventId / GeneratorConfig.PROPORTION_DENOMINATOR;
    long offset = eventId % GeneratorConfig.PROPORTION_DENOMINATOR;
    if (offset < GeneratorConfig.PERSON_PROPORTION) {
      // About to generate a person.
      // Go back to the last auction in the last epoch.
      epoch--;
      offset = GeneratorConfig.AUCTION_PROPORTION - 1;
    } else if (offset >= GeneratorConfig.PERSON_PROPORTION + GeneratorConfig.AUCTION_PROPORTION) {
      // About to generate a bid.
      // Go back to the last auction generated in this epoch.
      offset = GeneratorConfig.AUCTION_PROPORTION - 1;
    } else {
      // About to generate an auction.
      offset -= GeneratorConfig.PERSON_PROPORTION;
    }
    return epoch * GeneratorConfig.AUCTION_PROPORTION + offset;
  }

  /** return a random US state. */
  private static String nextUSState(Random random) {
    return US_STATES.get(random.nextInt(US_STATES.size()));
  }

  /** Return a random US city. */
  private static String nextUSCity(Random random) {
    return US_CITIES.get(random.nextInt(US_CITIES.size()));
  }

  /** Return a random person name. */
  private static String nextPersonName(Random random) {
    return FIRST_NAMES.get(random.nextInt(FIRST_NAMES.size())) + " "
        + LAST_NAMES.get(random.nextInt(LAST_NAMES.size()));
  }

  /** Return a random string of up to {@code maxLength}. */
  private static String nextString(Random random, int maxLength) {
    int len = MIN_STRING_LENGTH + random.nextInt(maxLength - MIN_STRING_LENGTH);
    StringBuilder sb = new StringBuilder();
    while (len-- > 0) {
      if (random.nextInt(13) == 0) {
        sb.append(' ');
      } else {
        sb.append((char) ('a' + random.nextInt(26)));
      }
    }
    return sb.toString().trim();
  }

  /** Return a random string of exactly {@code length}. */
  private static String nextExactString(Random random, int length) {
    StringBuilder sb = new StringBuilder();
    while (length-- > 0) {
      sb.append((char) ('a' + random.nextInt(26)));
    }
    return sb.toString();
  }

  /** Return a random email address. */
  private static String nextEmail(Random random) {
    return nextString(random, 7) + "@" + nextString(random, 5) + ".com";
  }

  /** Return a random credit card number. */
  private static String nextCreditCard(Random random) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 4; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(String.format("%04d", random.nextInt(10000)));
    }
    return sb.toString();
  }

  /** Return a random price. */
  private static long nextPrice(Random random) {
    return Math.round(Math.pow(10.0, random.nextDouble() * 6.0) * 100.0);
  }

  /** Return a random time delay, in milliseconds, for length of auctions. */
  private long nextAuctionLengthMs(Random random, long timestamp) {
    // What's our current event number?
    long currentEventNumber = config.nextAdjustedEventNumber(numEvents);
    // How many events till we've generated numInFlightAuctions?
    long numEventsForAuctions =
        (config.configuration.numInFlightAuctions * GeneratorConfig.PROPORTION_DENOMINATOR)
        / GeneratorConfig.AUCTION_PROPORTION;
    // When will the auction numInFlightAuctions beyond now be generated?
    long futureAuction =
        config.timestampAndInterEventDelayUsForEvent(currentEventNumber + numEventsForAuctions)
            .getKey();
    // System.out.printf("*** auction will be for %dms (%d events ahead) ***\n",
    //     futureAuction - timestamp, numEventsForAuctions);
    // Choose a length with average horizonMs.
    long horizonMs = futureAuction - timestamp;
    return 1L + nextLong(random, Math.max(horizonMs * 2, 1L));
  }

  /**
   * Return a random {@code string} such that {@code currentSize + string.length()} is on average
   * {@code averageSize}.
   */
  private static String nextExtra(Random random, int currentSize, int desiredAverageSize) {
    if (currentSize > desiredAverageSize) {
      return "";
    }
    desiredAverageSize -= currentSize;
    int delta = (int) Math.round(desiredAverageSize * 0.2);
    int minSize = desiredAverageSize - delta;
    int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));
    return nextExactString(random, desiredSize);
  }

  /** Return a random long from {@code [0, n)}. */
  private static long nextLong(Random random, long n) {
    if (n < Integer.MAX_VALUE) {
      return random.nextInt((int) n);
    } else {
      // WARNING: Very skewed distribution! Bad!
      return Math.abs(random.nextLong() % n);
    }
  }

  /**
   * Generate and return a random person with next available id.
   */
  private Person nextPerson(Random random, long timestamp) {
    long id = lastBase0PersonId() + GeneratorConfig.FIRST_PERSON_ID;
    String name = nextPersonName(random);
    String email = nextEmail(random);
    String creditCard = nextCreditCard(random);
    String city = nextUSCity(random);
    String state = nextUSState(random);
    int currentSize =
        8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
    String extra = nextExtra(random, currentSize, config.configuration.avgPersonByteSize);
    return new Person(id, name, email, creditCard, city, state, timestamp, extra);
  }

  /**
   * Return a random person id (base 0).
   */
  private long nextBase0PersonId(Random random) {
    // Choose a random person from any of the 'active' people, plus a few 'leads'.
    // By limiting to 'active' we ensure the density of bids or auctions per person
    // does not decrease over time for long running jobs.
    // By choosing a person id ahead of the last valid person id we will make
    // newPerson and newAuction events appear to have been swapped in time.
    long numPeople = lastBase0PersonId() + 1;
    long activePeople = Math.min(numPeople, config.configuration.numActivePeople);
    long n = nextLong(random, activePeople + PERSON_ID_LEAD);
    return numPeople - activePeople + n;
  }

  /**
   * Return a random auction id (base 0).
   */
  private long nextBase0AuctionId(Random random) {
    // Choose a random auction for any of those which are likely to still be in flight,
    // plus a few 'leads'.
    // Note that ideally we'd track non-expired auctions exactly, but that state
    // is difficult to split.
    long minAuction = Math.max(lastBase0AuctionId() - config.configuration.numInFlightAuctions, 0);
    long maxAuction = lastBase0AuctionId();
    return minAuction + nextLong(random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
  }

  /**
   * Generate and return a random auction with next available id.
   */
  private Auction nextAuction(Random random, long timestamp) {
    long id = lastBase0AuctionId() + GeneratorConfig.FIRST_AUCTION_ID;

    long seller;
    // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
    if (random.nextInt(config.configuration.hotSellersRatio) > 0) {
      // Choose the first person in the batch of last HOT_SELLER_RATIO people.
      seller = (lastBase0PersonId() / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
    } else {
      seller = nextBase0PersonId(random);
    }
    seller += GeneratorConfig.FIRST_PERSON_ID;

    long category = GeneratorConfig.FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
    long initialBid = nextPrice(random);
    long expires = timestamp + nextAuctionLengthMs(random, timestamp);
    String name = nextString(random, 20);
    String desc = nextString(random, 100);
    long reserve = initialBid + nextPrice(random);
    int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
    String extra = nextExtra(random, currentSize, config.configuration.avgAuctionByteSize);
    return new Auction(id, name, desc, initialBid, reserve, timestamp, expires, seller, category,
        extra);
  }

  /**
   * Generate and return a random bid with next available id.
   */
  private Bid nextBid(Random random, long timestamp) {
    long auction;
    // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
    if (random.nextInt(config.configuration.hotAuctionRatio) > 0) {
      // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
      auction = (lastBase0AuctionId() / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;
    } else {
      auction = nextBase0AuctionId(random);
    }
    auction += GeneratorConfig.FIRST_AUCTION_ID;

    long bidder;
    // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
    if (random.nextInt(config.configuration.hotBiddersRatio) > 0) {
      // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
      // last HOT_BIDDER_RATIO people.
      bidder = (lastBase0PersonId() / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
    } else {
      bidder = nextBase0PersonId(random);
    }
    bidder += GeneratorConfig.FIRST_PERSON_ID;

    long price = nextPrice(random);
    int currentSize = 8 + 8 + 8 + 8;
    String extra = nextExtra(random, currentSize, config.configuration.avgBidByteSize);
    return new Bid(auction, bidder, price, timestamp, extra);
  }

  @Override
  public boolean hasNext() {
    return numEvents < config.maxEvents;
  }

  /**
   * Return the next event. The outer timestamp is in wallclock time and corresponds to
   * when the event should fire. The inner timestamp is in event-time and represents the
   * time the event is purported to have taken place in the simulation.
   */
  public NextEvent nextEvent() {
    if (wallclockBaseTime < 0) {
      wallclockBaseTime = System.currentTimeMillis();
    }
    // When, in event time, we should generate the event. Monotonic.
    long eventTimestamp =
        config.timestampAndInterEventDelayUsForEvent(config.nextEventNumber(numEvents)).getKey();
    // When, in event time, the event should say it was generated. Depending on outOfOrderGroupSize
    // may have local jitter.
    long adjustedEventTimestamp =
        config.timestampAndInterEventDelayUsForEvent(config.nextAdjustedEventNumber(numEvents))
            .getKey();
    // The minimum of this and all future adjusted event timestamps. Accounts for jitter in
    // the event timestamp.
    long watermark =
        config.timestampAndInterEventDelayUsForEvent(config.nextEventNumberForWatermark(numEvents))
            .getKey();
    // When, in wallclock time, we should emit the event.
    long wallclockTimestamp = wallclockBaseTime + (eventTimestamp - getCurrentConfig().baseTime);

    // Seed the random number generator with the next 'event id'.
    Random random = new Random(getNextEventId());
    long rem = getNextEventId() % GeneratorConfig.PROPORTION_DENOMINATOR;

    Event event;
    if (rem < GeneratorConfig.PERSON_PROPORTION) {
      event = new Event(nextPerson(random, adjustedEventTimestamp));
    } else if (rem < GeneratorConfig.PERSON_PROPORTION + GeneratorConfig.AUCTION_PROPORTION) {
      event = new Event(nextAuction(random, adjustedEventTimestamp));
    } else {
      event = new Event(nextBid(random, adjustedEventTimestamp));
    }

    numEvents++;
    return new NextEvent(wallclockTimestamp, adjustedEventTimestamp, event, watermark);
  }

  @Override
  public TimestampedValue<Event> next() {
    NextEvent next = nextEvent();
    return TimestampedValue.of(next.event, new Instant(next.eventTimestamp));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Return how many microseconds till we emit the next event.
   */
  public long currentInterEventDelayUs() {
    return config.timestampAndInterEventDelayUsForEvent(config.nextEventNumber(numEvents))
        .getValue();
  }

  /**
   * Return an estimate of fraction of output consumed.
   */
  public double getFractionConsumed() {
    return (double) numEvents / config.maxEvents;
  }

  @Override
  public String toString() {
    return String.format("Generator{config:%s; numEvents:%d; wallclockBaseTime:%d}", config,
        numEvents, wallclockBaseTime);
  }
}
