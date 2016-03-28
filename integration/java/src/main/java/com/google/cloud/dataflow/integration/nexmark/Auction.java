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

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * An auction submitted by a person.
 */
public class Auction implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<Auction> CODER = new AtomicCoder<Auction>() {
    @Override
    public void encode(Auction value, OutputStream outStream,
        com.google.cloud.dataflow.sdk.coders.Coder.Context context)
        throws CoderException, IOException {
      LONG_CODER.encode(value.id, outStream, context.nested());
      STRING_CODER.encode(value.itemName, outStream, context.nested());
      STRING_CODER.encode(value.description, outStream, context.nested());
      LONG_CODER.encode(value.initialBid, outStream, context.nested());
      LONG_CODER.encode(value.reserve, outStream, context.nested());
      LONG_CODER.encode(value.dateTime, outStream, context.nested());
      LONG_CODER.encode(value.expires, outStream, context.nested());
      LONG_CODER.encode(value.seller, outStream, context.nested());
      LONG_CODER.encode(value.category, outStream, context.nested());
      STRING_CODER.encode(value.extra, outStream, context.nested());
    }

    @Override
    public Auction decode(
        InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
        throws CoderException, IOException {
      long id = LONG_CODER.decode(inStream, context.nested());
      String itemName = STRING_CODER.decode(inStream, context.nested());
      String description = STRING_CODER.decode(inStream, context.nested());
      long initialBid = LONG_CODER.decode(inStream, context.nested());
      long reserve = LONG_CODER.decode(inStream, context.nested());
      long dateTime = LONG_CODER.decode(inStream, context.nested());
      long expires = LONG_CODER.decode(inStream, context.nested());
      long seller = LONG_CODER.decode(inStream, context.nested());
      long category = LONG_CODER.decode(inStream, context.nested());
      String extra = STRING_CODER.decode(inStream, context.nested());
      return new Auction(
          id, itemName, description, initialBid, reserve, dateTime, expires, seller, category,
          extra);
    }
  };


  /** Id of auction. */
  @JsonProperty
  public final long id; // primary key

  /** Extra auction properties. */
  @JsonProperty
  public final String itemName;

  @JsonProperty
  public final String description;

  /** Initial bid price, in cents. */
  @JsonProperty
  public final long initialBid;

  /** Reserve price, in cents. */
  @JsonProperty
  public final long reserve;

  @JsonProperty
  public final long dateTime;

  /** When does auction expire? (ms since epoch). Bids at or after this time are ignored. */
  @JsonProperty
  public final long expires;

  /** Id of person who instigated auction. */
  @JsonProperty
  public final long seller; // foreign key: Person.id

  /** Id of category auction is listed under. */
  @JsonProperty
  public final long category; // foreign key: Category.id

  /** Additional arbitrary payload for performance testing. */
  @JsonProperty
  public final String extra;


  // For Avro only.
  @SuppressWarnings("unused")
  private Auction() {
    id = 0;
    itemName = null;
    description = null;
    initialBid = 0;
    reserve = 0;
    dateTime = 0;
    expires = 0;
    seller = 0;
    category = 0;
    extra = null;
  }

  public Auction(long id, String itemName, String description, long initialBid, long reserve,
      long dateTime, long expires, long seller, long category, String extra) {
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

  /**
   * Return a copy of auction which capture the given annotation.
   * (Used for debugging).
   */
  public Auction withAnnotation(String annotation) {
    return new Auction(id, itemName, description, initialBid, reserve, dateTime, expires, seller,
        category, annotation + ": " + extra);
  }

  /**
   * Does auction have {@code annotation}? (Used for debugging.)
   */
  public boolean hasAnnotation(String annotation) {
    return extra.startsWith(annotation + ": ");
  }

  /**
   * Remove {@code annotation} from auction. (Used for debugging.)
   */
  public Auction withoutAnnotation(String annotation) {
    if (hasAnnotation(annotation)) {
      return new Auction(id, itemName, description, initialBid, reserve, dateTime, expires, seller,
          category, extra.substring(annotation.length() + 2));
    } else {
      return this;
    }
  }

  @Override
  public long sizeInBytes() {
    return 8 + itemName.length() + 1 + description.length() + 1 + 8 + 8 + 8 + 8 + 8 + 8
        + extra.length() + 1;
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
