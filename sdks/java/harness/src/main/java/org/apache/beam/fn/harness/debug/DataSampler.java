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
package org.apache.beam.fn.harness.debug;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;

public class DataSampler {

  public DataSampler() {}

  public DataSampler(int maxSamples, int sampleEveryN) {
    this.maxSamples = maxSamples;
    this.sampleEveryN = sampleEveryN;
  }

  public static Set<String> EMPTY = new HashSet<>();

  // Maximum number of elements in buffer.
  private int maxSamples = 10;

  // Sampling rate.
  private int sampleEveryN = 1000;

  private final Map<String, Map<String, OutputSampler<?>>> outputSamplers = new HashMap<>();

  public <T> OutputSampler<T> sampleOutput(
      String processBundleDescriptorId, String pcollectionId, Coder<T> coder) {
    outputSamplers.putIfAbsent(processBundleDescriptorId, new HashMap<>());
    Map<String, OutputSampler<?>> samplers = outputSamplers.get(processBundleDescriptorId);
    samplers.putIfAbsent(
        pcollectionId, new OutputSampler<T>(coder, this.maxSamples, this.sampleEveryN));

    return (OutputSampler<T>) samplers.get(pcollectionId);
  }

  public Map<String, List<byte[]>> samples() {
    return samplesFor(EMPTY, EMPTY);
  }

  public Map<String, List<byte[]>> samplesFor(Set<String> descriptors, Set<String> pcollections) {
    Map<String, List<byte[]>> samples = new HashMap<>();
    outputSamplers.forEach(
        (descriptorId, samplers) -> {
          if (!descriptors.isEmpty() && !descriptors.contains(descriptorId)) {
            return;
          }

          samplers.forEach(
              (pcollectionId, outputSampler) -> {
                if (!pcollections.isEmpty() && !pcollections.contains(pcollectionId)) {
                  return;
                }

                samples.putIfAbsent(pcollectionId, new ArrayList<>());
                samples.get(pcollectionId).addAll(outputSampler.samples());
              });
        });

    return samples;
  }

  public Map<String, List<byte[]>> samplesForDescriptors(Set<String> descriptors) {
    return samplesFor(descriptors, EMPTY);
  }

  public Map<String, List<byte[]>> samplesForPCollections(Set<String> pcollections) {
    return samplesFor(EMPTY, pcollections);
  }
}
