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
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Result of Query4. */
public class CategoryPrice implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<Integer> INT_CODER = VarIntCoder.of();

  public static final Coder<CategoryPrice> CODER =
      new CustomCoder<CategoryPrice>() {
        @Override
        public void encode(CategoryPrice value, OutputStream outStream)
            throws CoderException, IOException {
          LONG_CODER.encode(value.category, outStream);
          LONG_CODER.encode(value.price, outStream);
          INT_CODER.encode(value.isLast ? 1 : 0, outStream);
        }

        @Override
        public CategoryPrice decode(InputStream inStream) throws CoderException, IOException {
          long category = LONG_CODER.decode(inStream);
          long price = LONG_CODER.decode(inStream);
          boolean isLast = INT_CODER.decode(inStream) != 0;
          return new CategoryPrice(category, price, isLast);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(CategoryPrice v) {
          return v;
        }
      };

  @JsonProperty public final long category;

  /** Price in cents. */
  @JsonProperty public final long price;

  @JsonProperty public final boolean isLast;

  // For Avro only.
  @SuppressWarnings("unused")
  private CategoryPrice() {
    category = 0;
    price = 0;
    isLast = false;
  }

  public CategoryPrice(long category, long price, boolean isLast) {
    this.category = category;
    this.price = price;
    this.isLast = isLast;
  }

  @Override
  public long sizeInBytes() {
    return 8L + 8L + 1L;
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
    CategoryPrice that = (CategoryPrice) o;
    return category == that.category && price == that.price && isLast == that.isLast;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(category, price, isLast);
  }
}
