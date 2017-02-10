/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * Custom Function for Hashing. The combiner will be combineUnordered, and accumulator is a
 * HashCode.
 */
public class HashingFn extends CombineFn<String, HashingFn.Accum, String> {
  public static class Accum {
    HashCode hashCode = null;

  }

  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum accum, String input) {
    List<HashCode> elementHashes = Lists.newArrayList();
    if (accum.hashCode != null) {
      elementHashes.add(accum.hashCode);
     }
    HashCode inputHashCode = Hashing.sha1().hashString(input, StandardCharsets.UTF_8);
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
  public Coder getAccumulatorCoder(CoderRegistry registry, Coder<String> inputCoder)
      throws CannotProvideCoderException {
    return registry.getCoder(new TypeDescriptor(Accum.class) {});
  }

  @Override
  public Coder getDefaultOutputCoder(CoderRegistry registry, Coder<String> inputCoder) {
    return inputCoder;
  }
}
