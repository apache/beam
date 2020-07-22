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
package org.apache.beam.sdk.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** A bid for an item on auction. */
@DefaultSchema(JavaFieldSchema.class)
public class Bid implements KnownSize, Serializable {
  private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<Bid> CODER =
      new CustomCoder<Bid>() {
        @Override
        public void encode(Bid value, OutputStream outStream) throws CoderException, IOException {
          LONG_CODER.encode(value.auction, outStream);
          LONG_CODER.encode(value.bidder, outStream);
          LONG_CODER.encode(value.price, outStream);
          INSTANT_CODER.encode(value.dateTime, outStream);
          STRING_CODER.encode(value.extra, outStream);
        }

        @Override
        public Bid decode(InputStream inStream) throws CoderException, IOException {
          long auction = LONG_CODER.decode(inStream);
          long bidder = LONG_CODER.decode(inStream);
          long price = LONG_CODER.decode(inStream);
          Instant dateTime = INSTANT_CODER.decode(inStream);
          String extra = STRING_CODER.decode(inStream);
          return new Bid(auction, bidder, price, dateTime, extra);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(Bid v) {
          return v;
        }
      };

  /**
   * Comparator to order bids by ascending price then descending time (for finding winning bids).
   */
  public static final Comparator<Bid> PRICE_THEN_DESCENDING_TIME =
      (left, right) -> {
        int i = Double.compare(left.price, right.price);
        if (i != 0) {
          return i;
        }
        return right.dateTime.compareTo(left.dateTime);
      };

  /**
   * Comparator to order bids by ascending time then ascending price. (for finding most recent
   * bids).
   */
  public static final Comparator<Bid> ASCENDING_TIME_THEN_PRICE =
      (left, right) -> {
        int i = left.dateTime.compareTo(right.dateTime);
        if (i != 0) {
          return i;
        }
        return Double.compare(left.price, right.price);
      };

  /** Id of auction this bid is for. */
  @JsonProperty public long auction; // foreign key: Auction.id

  /** Id of person bidding in auction. */
  @JsonProperty public long bidder; // foreign key: Person.id

  /** Price of bid, in cents. */
  @JsonProperty public long price;

  /**
   * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
   * event time.
   */
  @JsonProperty public Instant dateTime;

  /** Additional arbitrary payload for performance testing. */
  @JsonProperty public String extra;

  // For Avro only.
  @SuppressWarnings("unused")
  public Bid() {
    auction = 0;
    bidder = 0;
    price = 0;
    dateTime = null;
    extra = null;
  }

  public Bid(long auction, long bidder, long price, Instant dateTime, String extra) {
    this.auction = auction;
    this.bidder = bidder;
    this.price = price;
    this.dateTime = dateTime;
    this.extra = extra;
  }

  /** Return a copy of bid which capture the given annotation. (Used for debugging). */
  public Bid withAnnotation(String annotation) {
    return new Bid(auction, bidder, price, dateTime, annotation + ": " + extra);
  }

  /** Does bid have {@code annotation}? (Used for debugging.) */
  public boolean hasAnnotation(String annotation) {
    return extra.startsWith(annotation + ": ");
  }

  /** Remove {@code annotation} from bid. (Used for debugging.) */
  public Bid withoutAnnotation(String annotation) {
    if (hasAnnotation(annotation)) {
      return new Bid(auction, bidder, price, dateTime, extra.substring(annotation.length() + 2));
    } else {
      return this;
    }
  }

  @Override
  public boolean equals(@Nullable Object otherObject) {
    if (this == otherObject) {
      return true;
    }
    if (otherObject == null || getClass() != otherObject.getClass()) {
      return false;
    }

    Bid other = (Bid) otherObject;
    return Objects.equals(auction, other.auction)
        && Objects.equals(bidder, other.bidder)
        && Objects.equals(price, other.price)
        && Objects.equals(dateTime, other.dateTime)
        && Objects.equals(extra, other.extra);
  }

  @Override
  public int hashCode() {
    return Objects.hash(auction, bidder, price, dateTime, extra);
  }

  @Override
  public long sizeInBytes() {
    return 8L + 8L + 8L + 8L + extra.length() + 1L;
  }

  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
