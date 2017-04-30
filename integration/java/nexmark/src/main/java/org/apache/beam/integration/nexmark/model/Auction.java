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
package org.apache.beam.integration.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.integration.nexmark.NexmarkUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * An auction submitted by a person.
 */
public class Auction implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<Auction> CODER = new CustomCoder<Auction>() {
    @Override
    public void encode(Auction value, OutputStream outStream,
        Coder.Context context)
        throws CoderException, IOException {
      LONG_CODER.encode(value.id, outStream, Context.NESTED);
      STRING_CODER.encode(value.itemName, outStream, Context.NESTED);
      STRING_CODER.encode(value.description, outStream, Context.NESTED);
      LONG_CODER.encode(value.initialBid, outStream, Context.NESTED);
      LONG_CODER.encode(value.reserve, outStream, Context.NESTED);
      LONG_CODER.encode(value.dateTime, outStream, Context.NESTED);
      LONG_CODER.encode(value.expires, outStream, Context.NESTED);
      LONG_CODER.encode(value.seller, outStream, Context.NESTED);
      LONG_CODER.encode(value.category, outStream, Context.NESTED);
      STRING_CODER.encode(value.extra, outStream, Context.NESTED);
    }

    @Override
    public Auction decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      long id = LONG_CODER.decode(inStream, Context.NESTED);
      String itemName = STRING_CODER.decode(inStream, Context.NESTED);
      String description = STRING_CODER.decode(inStream, Context.NESTED);
      long initialBid = LONG_CODER.decode(inStream, Context.NESTED);
      long reserve = LONG_CODER.decode(inStream, Context.NESTED);
      long dateTime = LONG_CODER.decode(inStream, Context.NESTED);
      long expires = LONG_CODER.decode(inStream, Context.NESTED);
      long seller = LONG_CODER.decode(inStream, Context.NESTED);
      long category = LONG_CODER.decode(inStream, Context.NESTED);
      String extra = STRING_CODER.decode(inStream, Context.NESTED);
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
  private final String itemName;

  @JsonProperty
  private final String description;

  /** Initial bid price, in cents. */
  @JsonProperty
  private final long initialBid;

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
  private final String extra;


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
