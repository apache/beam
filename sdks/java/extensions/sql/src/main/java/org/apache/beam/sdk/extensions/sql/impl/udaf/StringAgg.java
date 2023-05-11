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
package org.apache.beam.sdk.extensions.sql.impl.udaf;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * {@link CombineFn}s for aggregating strings or bytes with an optional delimiter (default comma).
 *
 * <p>{@link StringAgg.StringAggString} does not support NULL values.
 */
public class StringAgg {

  /** A {@link CombineFn} that aggregates strings with a string as delimiter. */
  public static class StringAggString extends CombineFn<String, String, String> {
    private final String delimiter;

    public StringAggString(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public String createAccumulator() {
      return "";
    }

    @Override
    public String addInput(String curString, String nextString) {

      if (!nextString.isEmpty()) {
        if (!curString.isEmpty()) {
          curString += delimiter + nextString;
        } else {
          curString = nextString;
        }
      }

      return curString;
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumList) {
      String mergeString = "";
      for (String stringAccum : accumList) {
        if (!stringAccum.isEmpty()) {
          if (!mergeString.isEmpty()) {
            mergeString += delimiter + stringAccum;
          } else {
            mergeString = stringAccum;
          }
        }
      }

      return mergeString;
    }

    @Override
    public String extractOutput(String output) {
      return output;
    }
  }

  /** A {@link CombineFn} that aggregates bytes with a byte array as delimiter. */
  public static class StringAggByte extends CombineFn<byte[], String, byte[]> {
    private final String delimiter;

    public StringAggByte(byte[] delimiter) {
      this.delimiter = new String(delimiter, StandardCharsets.UTF_8);
    }

    @Override
    public String createAccumulator() {
      return "";
    }

    @Override
    public String addInput(String mutableAccumulator, byte[] input) {
      if (input != null) {
        if (!mutableAccumulator.isEmpty()) {
          mutableAccumulator += delimiter + new String(input, StandardCharsets.UTF_8);
        } else {
          mutableAccumulator = new String(input, StandardCharsets.UTF_8);
        }
      }
      return mutableAccumulator;
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumList) {
      String mergeString = "";
      for (String stringAccum : accumList) {
        if (!stringAccum.isEmpty()) {
          if (!mergeString.isEmpty()) {
            mergeString += delimiter + stringAccum;
          } else {
            mergeString = stringAccum;
          }
        }
      }

      return mergeString;
    }

    @Override
    public byte[] extractOutput(String output) {
      return output.getBytes(StandardCharsets.UTF_8);
    }
  }
}
