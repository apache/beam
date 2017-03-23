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
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * A person either creating an auction or making a bid.
 */
public class Person implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
  public static final Coder<Person> CODER = new AtomicCoder<Person>() {
    @Override
    public void encode(Person value, OutputStream outStream,
        Coder.Context context)
        throws CoderException, IOException {
      LONG_CODER.encode(value.id, outStream, Context.NESTED);
      STRING_CODER.encode(value.name, outStream, Context.NESTED);
      STRING_CODER.encode(value.emailAddress, outStream, Context.NESTED);
      STRING_CODER.encode(value.creditCard, outStream, Context.NESTED);
      STRING_CODER.encode(value.city, outStream, Context.NESTED);
      STRING_CODER.encode(value.state, outStream, Context.NESTED);
      LONG_CODER.encode(value.dateTime, outStream, Context.NESTED);
      STRING_CODER.encode(value.extra, outStream, Context.NESTED);
    }

    @Override
    public Person decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      long id = LONG_CODER.decode(inStream, Context.NESTED);
      String name = STRING_CODER.decode(inStream, Context.NESTED);
      String emailAddress = STRING_CODER.decode(inStream, Context.NESTED);
      String creditCard = STRING_CODER.decode(inStream, Context.NESTED);
      String city = STRING_CODER.decode(inStream, Context.NESTED);
      String state = STRING_CODER.decode(inStream, Context.NESTED);
      long dateTime = LONG_CODER.decode(inStream, Context.NESTED);
      String extra = STRING_CODER.decode(inStream, Context.NESTED);
      return new Person(id, name, emailAddress, creditCard, city, state, dateTime, extra);
    }
  };

  /** Id of person. */
  @JsonProperty
  public final long id; // primary key

  /** Extra person properties. */
  @JsonProperty
  public final String name;

  @JsonProperty
  public final String emailAddress;

  @JsonProperty
  public final String creditCard;

  @JsonProperty
  public final String city;

  @JsonProperty
  public final String state;

  @JsonProperty
  public final long dateTime;

  /** Additional arbitrary payload for performance testing. */
  @JsonProperty
  public final String extra;

  // For Avro only.
  @SuppressWarnings("unused")
  private Person() {
    id = 0;
    name = null;
    emailAddress = null;
    creditCard = null;
    city = null;
    state = null;
    dateTime = 0;
    extra = null;
  }

  public Person(long id, String name, String emailAddress, String creditCard, String city,
      String state, long dateTime, String extra) {
    this.id = id;
    this.name = name;
    this.emailAddress = emailAddress;
    this.creditCard = creditCard;
    this.city = city;
    this.state = state;
    this.dateTime = dateTime;
    this.extra = extra;
  }

  /**
   * Return a copy of person which capture the given annotation.
   * (Used for debugging).
   */
  public Person withAnnotation(String annotation) {
    return new Person(id, name, emailAddress, creditCard, city, state, dateTime,
        annotation + ": " + extra);
  }

  /**
   * Does person have {@code annotation}? (Used for debugging.)
   */
  public boolean hasAnnotation(String annotation) {
    return extra.startsWith(annotation + ": ");
  }

  /**
   * Remove {@code annotation} from person. (Used for debugging.)
   */
  public Person withoutAnnotation(String annotation) {
    if (hasAnnotation(annotation)) {
      return new Person(id, name, emailAddress, creditCard, city, state, dateTime,
          extra.substring(annotation.length() + 2));
    } else {
      return this;
    }
  }

  @Override
  public long sizeInBytes() {
    return 8 + name.length() + 1 + emailAddress.length() + 1 + creditCard.length() + 1
        + city.length() + 1 + state.length() + 8 + 1 + extra.length() + 1;
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
