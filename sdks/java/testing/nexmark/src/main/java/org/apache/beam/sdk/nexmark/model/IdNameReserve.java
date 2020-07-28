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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Result type of Query8. */
public class IdNameReserve implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<IdNameReserve> CODER =
      new CustomCoder<IdNameReserve>() {
        @Override
        public void encode(IdNameReserve value, OutputStream outStream)
            throws CoderException, IOException {
          LONG_CODER.encode(value.id, outStream);
          STRING_CODER.encode(value.name, outStream);
          LONG_CODER.encode(value.reserve, outStream);
        }

        @Override
        public IdNameReserve decode(InputStream inStream) throws CoderException, IOException {
          long id = LONG_CODER.decode(inStream);
          String name = STRING_CODER.decode(inStream);
          long reserve = LONG_CODER.decode(inStream);
          return new IdNameReserve(id, name, reserve);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(IdNameReserve v) {
          return v;
        }
      };

  @JsonProperty private final long id;

  @JsonProperty private final String name;

  /** Reserve price in cents. */
  @JsonProperty private final long reserve;

  // For Avro only.
  @SuppressWarnings("unused")
  private IdNameReserve() {
    id = 0;
    name = null;
    reserve = 0;
  }

  public IdNameReserve(long id, String name, long reserve) {
    this.id = id;
    this.name = name;
    this.reserve = reserve;
  }

  @Override
  public long sizeInBytes() {
    return 8L + name.length() + 1L + 8L;
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
    IdNameReserve that = (IdNameReserve) o;
    return id == that.id && reserve == that.reserve && Objects.equal(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, reserve);
  }
}
