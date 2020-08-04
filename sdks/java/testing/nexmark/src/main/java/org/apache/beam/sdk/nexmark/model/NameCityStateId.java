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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Result of Query3. */
@DefaultSchema(JavaFieldSchema.class)
public class NameCityStateId implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<NameCityStateId> CODER =
      new CustomCoder<NameCityStateId>() {
        @Override
        public void encode(NameCityStateId value, OutputStream outStream)
            throws CoderException, IOException {
          STRING_CODER.encode(value.name, outStream);
          STRING_CODER.encode(value.city, outStream);
          STRING_CODER.encode(value.state, outStream);
          LONG_CODER.encode(value.id, outStream);
        }

        @Override
        public NameCityStateId decode(InputStream inStream) throws CoderException, IOException {
          String name = STRING_CODER.decode(inStream);
          String city = STRING_CODER.decode(inStream);
          String state = STRING_CODER.decode(inStream);
          long id = LONG_CODER.decode(inStream);
          return new NameCityStateId(name, city, state, id);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(NameCityStateId v) {
          return v;
        }
      };

  @JsonProperty public String name;

  @JsonProperty public String city;

  @JsonProperty public String state;

  @JsonProperty public long id;

  // For Avro only.
  @SuppressWarnings("unused")
  public NameCityStateId() {
    name = null;
    city = null;
    state = null;
    id = 0;
  }

  public NameCityStateId(String name, String city, String state, long id) {
    this.name = name;
    this.city = city;
    this.state = state;
    this.id = id;
  }

  @Override
  public boolean equals(@Nullable Object otherObject) {
    if (this == otherObject) {
      return true;
    }
    if (otherObject == null || getClass() != otherObject.getClass()) {
      return false;
    }

    NameCityStateId other = (NameCityStateId) otherObject;
    return Objects.equals(name, other.name)
        && Objects.equals(city, other.city)
        && Objects.equals(state, other.state)
        && Objects.equals(id, other.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, city, state, id);
  }

  @Override
  public long sizeInBytes() {
    return name.length() + 1L + city.length() + 1L + state.length() + 1L + 8L;
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
