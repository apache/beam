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

package org.apache.beam.integration.nexmark;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarLongCoder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Result of {@link Query2}.
 */
public class AuctionPrice implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();

  public static final Coder<AuctionPrice> CODER = new AtomicCoder<AuctionPrice>() {
    @Override
    public void encode(AuctionPrice value, OutputStream outStream,
        Coder.Context context)
        throws CoderException, IOException {
      LONG_CODER.encode(value.auction, outStream, Context.NESTED);
      LONG_CODER.encode(value.price, outStream, Context.NESTED);
    }

    @Override
    public AuctionPrice decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      long auction = LONG_CODER.decode(inStream, Context.NESTED);
      long price = LONG_CODER.decode(inStream, Context.NESTED);
      return new AuctionPrice(auction, price);
    }
  };

  @JsonProperty
  public final long auction;

  /** Price in cents. */
  @JsonProperty
  public final long price;

  // For Avro only.
  @SuppressWarnings("unused")
  private AuctionPrice() {
    auction = 0;
    price = 0;
  }

  public AuctionPrice(long auction, long price) {
    this.auction = auction;
    this.price = price;
  }

  @Override
  public long sizeInBytes() {
    return 8 + 8;
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
