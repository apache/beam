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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;

/**
 * An event in the auction system, either a (new) {@link Person}, a (new) {@link Auction}, or a
 * {@link Bid}.
 */
public class Event implements KnownSize, Serializable {

  private enum Tag {
    PERSON(0),
    AUCTION(1),
    BID(2);

    private int value = -1;

    Tag(int value) {
      this.value = value;
    }
  }

  private static final Coder<Integer> INT_CODER = VarIntCoder.of();

  public static final Coder<Event> CODER =
      new CustomCoder<Event>() {
        @Override
        public void encode(Event value, OutputStream outStream) throws IOException {
          if (value.newPerson != null) {
            INT_CODER.encode(Tag.PERSON.value, outStream);
            Person.CODER.encode(value.newPerson, outStream);
          } else if (value.newAuction != null) {
            INT_CODER.encode(Tag.AUCTION.value, outStream);
            Auction.CODER.encode(value.newAuction, outStream);
          } else if (value.bid != null) {
            INT_CODER.encode(Tag.BID.value, outStream);
            Bid.CODER.encode(value.bid, outStream);
          } else {
            throw new RuntimeException("invalid event");
          }
        }

        @Override
        public Event decode(InputStream inStream) throws IOException {
          int tag = INT_CODER.decode(inStream);
          if (tag == Tag.PERSON.value) {
            Person person = Person.CODER.decode(inStream);
            return new Event(person);
          } else if (tag == Tag.AUCTION.value) {
            Auction auction = Auction.CODER.decode(inStream);
            return new Event(auction);
          } else if (tag == Tag.BID.value) {
            Bid bid = Bid.CODER.decode(inStream);
            return new Event(bid);
          } else {
            throw new RuntimeException("invalid event encoding");
          }
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}
      };

  @Nullable
  @org.apache.avro.reflect.Nullable
  public final Person newPerson;

  @Nullable
  @org.apache.avro.reflect.Nullable
  public final Auction newAuction;

  @Nullable
  @org.apache.avro.reflect.Nullable
  public final Bid bid;

  // For Avro only.
  @SuppressWarnings("unused")
  private Event() {
    newPerson = null;
    newAuction = null;
    bid = null;
  }

  public Event(Person newPerson) {
    this.newPerson = newPerson;
    newAuction = null;
    bid = null;
  }

  public Event(Auction newAuction) {
    newPerson = null;
    this.newAuction = newAuction;
    bid = null;
  }

  public Event(Bid bid) {
    newPerson = null;
    newAuction = null;
    this.bid = bid;
  }

  /** Return a copy of event which captures {@code annotation}. (Used for debugging). */
  public Event withAnnotation(String annotation) {
    if (newPerson != null) {
      return new Event(newPerson.withAnnotation(annotation));
    } else if (newAuction != null) {
      return new Event(newAuction.withAnnotation(annotation));
    } else {
      return new Event(bid.withAnnotation(annotation));
    }
  }

  /** Does event have {@code annotation}? (Used for debugging.) */
  public boolean hasAnnotation(String annotation) {
    if (newPerson != null) {
      return newPerson.hasAnnotation(annotation);
    } else if (newAuction != null) {
      return newAuction.hasAnnotation(annotation);
    } else {
      return bid.hasAnnotation(annotation);
    }
  }

  @Override
  public long sizeInBytes() {
    if (newPerson != null) {
      return 1 + newPerson.sizeInBytes();
    } else if (newAuction != null) {
      return 1 + newAuction.sizeInBytes();
    } else if (bid != null) {
      return 1 + bid.sizeInBytes();
    } else {
      throw new RuntimeException("invalid event");
    }
  }

  @Override
  public String toString() {
    if (newPerson != null) {
      return newPerson.toString();
    } else if (newAuction != null) {
      return newAuction.toString();
    } else if (bid != null) {
      return bid.toString();
    } else {
      throw new RuntimeException("invalid event");
    }
  }
}
