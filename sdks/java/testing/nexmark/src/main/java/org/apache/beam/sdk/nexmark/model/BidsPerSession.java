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
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Result of query 11. */
public class BidsPerSession implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();

  public static final Coder<BidsPerSession> CODER =
      new CustomCoder<BidsPerSession>() {
        @Override
        public void encode(BidsPerSession value, OutputStream outStream)
            throws CoderException, IOException {
          LONG_CODER.encode(value.personId, outStream);
          LONG_CODER.encode(value.bidsPerSession, outStream);
        }

        @Override
        public BidsPerSession decode(InputStream inStream) throws CoderException, IOException {
          long personId = LONG_CODER.decode(inStream);
          long bidsPerSession = LONG_CODER.decode(inStream);
          return new BidsPerSession(personId, bidsPerSession);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(BidsPerSession v) {
          return v;
        }
      };

  @JsonProperty private final long personId;

  @JsonProperty private final long bidsPerSession;

  public BidsPerSession() {
    personId = 0;
    bidsPerSession = 0;
  }

  public BidsPerSession(long personId, long bidsPerSession) {
    this.personId = personId;
    this.bidsPerSession = bidsPerSession;
  }

  @Override
  public long sizeInBytes() {
    // Two longs.
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

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BidsPerSession that = (BidsPerSession) o;
    return personId == that.personId && bidsPerSession == that.bidsPerSession;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(personId, bidsPerSession);
  }
}
