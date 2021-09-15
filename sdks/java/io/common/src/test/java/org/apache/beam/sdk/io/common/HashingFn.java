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
package org.apache.beam.sdk.io.common;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Custom Function for Hashing. The combiner is combineUnordered, and accumulator is a HashCode. */
public class HashingFn extends CombineFn<String, HashingFn.Accum, String> {

  /** Serializable Class to store the HashCode of input String. */
  public static class Accum implements Serializable {
    HashCode hashCode = null;

    public Accum(HashCode value) {
      this.hashCode = value;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Accum accum = (Accum) o;

      return hashCode != null ? hashCode.equals(accum.hashCode) : accum.hashCode == null;
    }

    @Override
    public int hashCode() {
      return hashCode != null ? hashCode.hashCode() : 0;
    }
  }

  @Override
  public Accum addInput(Accum accum, String input) {
    List<HashCode> elementHashes = Lists.newArrayList();
    if (accum.hashCode != null) {
      elementHashes.add(accum.hashCode);
    }
    HashCode inputHashCode = Hashing.murmur3_128().hashString(input, StandardCharsets.UTF_8);
    elementHashes.add(inputHashCode);
    accum.hashCode = Hashing.combineUnordered(elementHashes);
    return accum;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accums) {
    Accum merged = createAccumulator();
    List<HashCode> elementHashes = Lists.newArrayList();
    for (Accum accum : accums) {
      if (accum.hashCode != null) {
        elementHashes.add(accum.hashCode);
      }
    }
    merged.hashCode = Hashing.combineUnordered(elementHashes);
    return merged;
  }

  @Override
  public String extractOutput(Accum accum) {
    // Return the combined hash code of list of elements in the Pcollection.
    String consolidatedHash = "";
    if (accum.hashCode != null) {
      consolidatedHash = accum.hashCode.toString();
    }
    return consolidatedHash;
  }

  @Override
  public Coder<Accum> getAccumulatorCoder(CoderRegistry registry, Coder<String> inputCoder)
      throws CannotProvideCoderException {
    return SerializableCoder.of(Accum.class);
  }

  @Override
  public Coder<String> getDefaultOutputCoder(CoderRegistry registry, Coder<String> inputCoder) {
    return inputCoder;
  }

  @Override
  public Accum createAccumulator() {
    return new Accum(null);
  }
}
