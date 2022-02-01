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
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Result of Query2. */
@DefaultSchema(JavaFieldSchema.class)
public class AuctionPrice implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();

  public static final Coder<AuctionPrice> CODER =
      new CustomCoder<AuctionPrice>() {
        @Override
        public void encode(AuctionPrice value, OutputStream outStream)
            throws CoderException, IOException {
          LONG_CODER.encode(value.auction, outStream);
          LONG_CODER.encode(value.price, outStream);
        }

        @Override
        public AuctionPrice decode(InputStream inStream) throws CoderException, IOException {
          long auction = LONG_CODER.decode(inStream);
          long price = LONG_CODER.decode(inStream);
          return new AuctionPrice(auction, price);
        }

        @Override
        public Object structuralValue(AuctionPrice v) {
          return v;
        }
      };

  @JsonProperty public long auction;

  /** Price in cents. */
  @JsonProperty public long price;

  @SuppressWarnings("unused")
  public AuctionPrice() {
    auction = 0;
    price = 0;
  }

  public AuctionPrice(long auction, long price) {
    this.auction = auction;
    this.price = price;
  }

  @Override
  public boolean equals(@Nullable Object otherObject) {
    if (this == otherObject) {
      return true;
    }
    if (otherObject == null || getClass() != otherObject.getClass()) {
      return false;
    }

    AuctionPrice other = (AuctionPrice) otherObject;
    return Objects.equals(auction, other.auction) && Objects.equals(price, other.price);
  }

  @Override
  public int hashCode() {
    return Objects.hash(auction, price);
  }

  @Override
  public long sizeInBytes() {
    return 8L + 8L;
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
