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
package org.apache.beam.sdk.io.hadoop.inputformat.testing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * Custom Function for Hashing. The combiner will be combineUnordered, and accumulator is a
 * HashCode.
 * 
 */
public class HashingFn extends CombineFn<String, HashingFn.Accum, String> {
  public static class Accum {
    List<String> pCollElements = new ArrayList<String>();
  }

  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum accum, String input) {
    accum.pCollElements.add(input);
    return accum;
  }



  @Override
  public Accum mergeAccumulators(Iterable<Accum> accums) {
    Accum merged = createAccumulator();
    for (Accum accum : accums) {
      merged.pCollElements = accum.pCollElements;
    }
    return merged;
  }

  @Override
  public String extractOutput(Accum accum) {
    return generateHash(accum.pCollElements);
  }

  /**
   * This method generates checksum for a list of Strings.
   * 
   */
  private String generateHash(@Nonnull List<String> inputs) {
    List<HashCode> rowHashes = Lists.newArrayList();
    for(String input : inputs){
      rowHashes.add(Hashing.sha1().hashString(input, StandardCharsets.UTF_8));
    }
    return Hashing.combineUnordered(rowHashes).toString();
  }

  @Override
  public Coder getAccumulatorCoder(CoderRegistry registry, Coder<String> inputCoder) {
    return StringDelegateCoder.of(String.class);
  }

  @Override
  public Coder getDefaultOutputCoder(CoderRegistry registry, Coder<String> inputCoder) {
    return StringDelegateCoder.of(String.class);
  }

}
