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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;

/**
 * An event in the auction system, either a (new) {@link Person}, a (new) {@link Auction},
 * or a {@link Bid}.
 */
public class Event implements KnownSize, Serializable {
  private static final Coder<Integer> INT_CODER = VarIntCoder.of();

  public static final Coder<Event> CODER = new AtomicCoder<Event>() {
    @Override
    public void encode(Event value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      if (value.newPerson != null) {
        INT_CODER.encode(0, outStream, Context.NESTED);
        Person.CODER.encode(value.newPerson, outStream, Context.NESTED);
      } else if (value.newAuction != null) {
        INT_CODER.encode(1, outStream, Context.NESTED);
        Auction.CODER.encode(value.newAuction, outStream, Context.NESTED);
      } else if (value.bid != null) {
        INT_CODER.encode(2, outStream, Context.NESTED);
        Bid.CODER.encode(value.bid, outStream, Context.NESTED);
      } else {
        throw new RuntimeException("invalid event");
      }
    }

    @Override
    public Event decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      int tag = INT_CODER.decode(inStream, context);
      if (tag == 0) {
        Person person = Person.CODER.decode(inStream, Context.NESTED);
        return new Event(person);
      } else if (tag == 1) {
        Auction auction = Auction.CODER.decode(inStream, Context.NESTED);
        return new Event(auction);
      } else if (tag == 2) {
        Bid bid = Bid.CODER.decode(inStream, Context.NESTED);
        return new Event(bid);
      } else {
        throw new RuntimeException("invalid event encoding");
      }
    }
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

  /**
   * Return a copy of event which captures {@code annotation}.
   * (Used for debugging).
   */
  public Event withAnnotation(String annotation) {
    if (newPerson != null) {
      return new Event(newPerson.withAnnotation(annotation));
    } else if (newAuction != null) {
      return new Event(newAuction.withAnnotation(annotation));
    } else {
      return new Event(bid.withAnnotation(annotation));
    }
  }

  /**
   * Does event have {@code annotation}? (Used for debugging.)
   */
  public boolean hasAnnotation(String annotation) {
    if (newPerson != null) {
      return newPerson.hasAnnotation(annotation);
    } else if (newAuction != null) {
      return newAuction.hasAnnotation(annotation);
    } else {
      return bid.hasAnnotation(annotation);
    }
  }

  /**
   * Remove {@code annotation} from event. (Used for debugging.)
   */
  public Event withoutAnnotation(String annotation) {
    if (newPerson != null) {
      return new Event(newPerson.withoutAnnotation(annotation));
    } else if (newAuction != null) {
      return new Event(newAuction.withoutAnnotation(annotation));
    } else {
      return new Event(bid.withoutAnnotation(annotation));
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
