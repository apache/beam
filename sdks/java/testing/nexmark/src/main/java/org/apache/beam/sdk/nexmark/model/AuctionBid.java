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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.queries.WinningBids;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Result of {@link WinningBids} transform. */
public class AuctionBid implements KnownSize, Serializable {
  public static final Coder<AuctionBid> CODER =
      new CustomCoder<AuctionBid>() {
        @Override
        public void encode(AuctionBid value, OutputStream outStream)
            throws CoderException, IOException {
          Auction.CODER.encode(value.auction, outStream);
          Bid.CODER.encode(value.bid, outStream);
        }

        @Override
        public AuctionBid decode(InputStream inStream) throws CoderException, IOException {
          Auction auction = Auction.CODER.decode(inStream);
          Bid bid = Bid.CODER.decode(inStream);
          return new AuctionBid(auction, bid);
        }

        @Override
        public Object structuralValue(AuctionBid v) {
          return v;
        }
      };

  @JsonProperty public Auction auction;

  @JsonProperty public Bid bid;

  @SuppressWarnings("unused")
  public AuctionBid() {
    auction = null;
    bid = null;
  }

  public AuctionBid(Auction auction, Bid bid) {
    this.auction = auction;
    this.bid = bid;
  }

  @Override
  public long sizeInBytes() {
    return auction.sizeInBytes() + bid.sizeInBytes();
  }

  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuctionBid that = (AuctionBid) o;
    return Objects.equal(auction, that.auction) && Objects.equal(bid, that.bid);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(auction, bid);
  }
}
