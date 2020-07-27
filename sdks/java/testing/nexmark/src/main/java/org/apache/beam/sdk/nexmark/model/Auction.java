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
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** An auction submitted by a person. */
@DefaultSchema(JavaFieldSchema.class)
public class Auction implements KnownSize, Serializable {
  private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<Auction> CODER =
      new CustomCoder<Auction>() {
        @Override
        public void encode(Auction value, OutputStream outStream)
            throws CoderException, IOException {
          LONG_CODER.encode(value.id, outStream);
          STRING_CODER.encode(value.itemName, outStream);
          STRING_CODER.encode(value.description, outStream);
          LONG_CODER.encode(value.initialBid, outStream);
          LONG_CODER.encode(value.reserve, outStream);
          INSTANT_CODER.encode(value.dateTime, outStream);
          INSTANT_CODER.encode(value.expires, outStream);
          LONG_CODER.encode(value.seller, outStream);
          LONG_CODER.encode(value.category, outStream);
          STRING_CODER.encode(value.extra, outStream);
        }

        @Override
        public Auction decode(InputStream inStream) throws CoderException, IOException {
          long id = LONG_CODER.decode(inStream);
          String itemName = STRING_CODER.decode(inStream);
          String description = STRING_CODER.decode(inStream);
          long initialBid = LONG_CODER.decode(inStream);
          long reserve = LONG_CODER.decode(inStream);
          Instant dateTime = INSTANT_CODER.decode(inStream);
          Instant expires = INSTANT_CODER.decode(inStream);
          long seller = LONG_CODER.decode(inStream);
          long category = LONG_CODER.decode(inStream);
          String extra = STRING_CODER.decode(inStream);
          return new Auction(
              id,
              itemName,
              description,
              initialBid,
              reserve,
              dateTime,
              expires,
              seller,
              category,
              extra);
        }

        @Override
        public Object structuralValue(Auction v) {
          return v;
        }
      };

  /** Id of auction. */
  @JsonProperty public long id; // primary key

  /** Extra auction properties. */
  @JsonProperty public String itemName;

  @JsonProperty public String description;

  /** Initial bid price, in cents. */
  @JsonProperty public long initialBid;

  /** Reserve price, in cents. */
  @JsonProperty public long reserve;

  @JsonProperty public Instant dateTime;

  /** When does auction expire? (ms since epoch). Bids at or after this time are ignored. */
  @JsonProperty public Instant expires;

  /** Id of person who instigated auction. */
  @JsonProperty public long seller; // foreign key: Person.id

  /** Id of category auction is listed under. */
  @JsonProperty public long category; // foreign key: Category.id

  /** Additional arbitrary payload for performance testing. */
  @JsonProperty public String extra;

  // For Avro only.
  @SuppressWarnings("unused")
  public Auction() {
    id = 0;
    itemName = null;
    description = null;
    initialBid = 0;
    reserve = 0;
    dateTime = null;
    expires = null;
    seller = 0;
    category = 0;
    extra = null;
  }

  public Auction(
      long id,
      String itemName,
      String description,
      long initialBid,
      long reserve,
      Instant dateTime,
      Instant expires,
      long seller,
      long category,
      String extra) {
    this.id = id;
    this.itemName = itemName;
    this.description = description;
    this.initialBid = initialBid;
    this.reserve = reserve;
    this.dateTime = dateTime;
    this.expires = expires;
    this.seller = seller;
    this.category = category;
    this.extra = extra;
  }

  /** Return a copy of auction which capture the given annotation. (Used for debugging). */
  public Auction withAnnotation(String annotation) {
    return new Auction(
        id,
        itemName,
        description,
        initialBid,
        reserve,
        dateTime,
        expires,
        seller,
        category,
        annotation + ": " + extra);
  }

  /** Does auction have {@code annotation}? (Used for debugging.) */
  public boolean hasAnnotation(String annotation) {
    return extra.startsWith(annotation + ": ");
  }

  /** Remove {@code annotation} from auction. (Used for debugging.) */
  public Auction withoutAnnotation(String annotation) {
    if (hasAnnotation(annotation)) {
      return new Auction(
          id,
          itemName,
          description,
          initialBid,
          reserve,
          dateTime,
          expires,
          seller,
          category,
          extra.substring(annotation.length() + 2));
    } else {
      return this;
    }
  }

  @Override
  public long sizeInBytes() {
    return 8L
        + itemName.length()
        + 1L
        + description.length()
        + 1L
        + 8L
        + 8L
        + 8L
        + 8L
        + 8L
        + 8L
        + extra.length()
        + 1L;
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
    Auction auction = (Auction) o;
    return id == auction.id
        && initialBid == auction.initialBid
        && reserve == auction.reserve
        && Objects.equal(dateTime, auction.dateTime)
        && Objects.equal(expires, auction.expires)
        && seller == auction.seller
        && category == auction.category
        && Objects.equal(itemName, auction.itemName)
        && Objects.equal(description, auction.description)
        && Objects.equal(extra, auction.extra);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra);
  }
}
