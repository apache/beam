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

import avro.shaded.com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.SampleDataResponse.ElementList;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.SampledElement;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;

/**
 * The DataSampler is a global (per SDK Harness) object that facilitates taking and returning
 * samples to the Runner Harness. The class is thread-safe with respect to executing
 * ProcessBundleDescriptors. Meaning, different threads executing different PBDs can sample
 * simultaneously, even if computing the same logical PCollection.
 */
public class DataSampler {

  /** Creates a DataSampler to sample every 10 elements while keeping a maximum of 10 in memory. */
  public DataSampler() {}

  /**
   * @param maxSamples Sets the maximum number of samples held in memory at once.
   * @param sampleEveryN Sets how often to sample.
   */
  public DataSampler(int maxSamples, int sampleEveryN) {
    this.maxSamples = maxSamples;
    this.sampleEveryN = sampleEveryN;
  }

  // Maximum number of elements in buffer.
  private int maxSamples = 10;

  // Sampling rate.
  private int sampleEveryN = 1000;

  // The fully-qualified type is: Map[ProcessBundleDescriptorId, [PCollectionId, OutputSampler]].
  // The DataSampler object lives on the same level of the FnHarness. This means that many threads
  // can and will
  // access this simultaneously. However, ProcessBundleDescriptors are unique per thread, so only
  // synchronization
  // is needed on the outermost map.
  private final Map<String, Map<String, OutputSampler<?>>> outputSamplers =
      new ConcurrentHashMap<>();

  /**
   * Creates and returns a class to sample the given PCollection in the given
   * ProcessBundleDescriptor. Uses the given coder encode samples as bytes when responding to a
   * SampleDataRequest.
   *
   * @param processBundleDescriptorId The PBD to sample from.
   * @param pcollectionId The PCollection to take intermittent samples from.
   * @param coder The coder associated with the PCollection. Coder may be from a nested context.
   * @return the OutputSampler corresponding to the unique PBD and PCollection.
   * @param <T> The type of element contained in the PCollection.
   */
  public <T> OutputSampler<T> sampleOutput(
      String processBundleDescriptorId, String pcollectionId, Coder<T> coder) {
    outputSamplers.putIfAbsent(processBundleDescriptorId, new HashMap<>());
    Map<String, OutputSampler<?>> samplers = outputSamplers.get(processBundleDescriptorId);
    samplers.putIfAbsent(
        pcollectionId, new OutputSampler<T>(coder, this.maxSamples, this.sampleEveryN));

    return (OutputSampler<T>) samplers.get(pcollectionId);
  }

  /**
   * Returns all collected samples. Thread-safe.
   *
   * @param request The instruction request from the FnApi. Filters based on the given
   *     SampleDataRequest.
   * @return Returns all collected samples.
   */
  public BeamFnApi.InstructionResponse.Builder handleDataSampleRequest(
      BeamFnApi.InstructionRequest request) {
    BeamFnApi.SampleDataRequest sampleDataRequest = request.getSample();

    Map<String, List<byte[]>> responseSamples =
        samplesFor(
            ImmutableSet.copyOf(sampleDataRequest.getProcessBundleDescriptorIdsList()),
            ImmutableSet.copyOf(sampleDataRequest.getPcollectionIdsList()));

    BeamFnApi.SampleDataResponse.Builder response = BeamFnApi.SampleDataResponse.newBuilder();
    for (String pcollectionId : responseSamples.keySet()) {
      ElementList.Builder elementList = ElementList.newBuilder();
      for (byte[] sample : responseSamples.get(pcollectionId)) {
        elementList.addElements(
            SampledElement.newBuilder().setElement(ByteString.copyFrom(sample)).build());
      }
      response.putElementSamples(pcollectionId, elementList.build());
    }

    return BeamFnApi.InstructionResponse.newBuilder().setSample(response);
  }

  /**
   * Returns a map from PCollection to its samples. Samples are filtered on
   * ProcessBundleDescriptorIds and PCollections. Thread-safe.
   *
   * @param descriptors PCollections under each PBD id will be unioned. If empty, allows all
   *     descriptors.
   * @param pcollections Filters all PCollections on this set. If empty, allows all PCollections.
   * @return a map from PCollection to its samples.
   */
  public Map<String, List<byte[]>> samplesFor(Set<String> descriptors, Set<String> pcollections) {
    Map<String, List<byte[]>> samples = new HashMap<>();

    // Safe to iterate as the ConcurrentHashMap will return each element at most once and will not
    // throw
    // ConcurrentModificationException.
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

  /** @return samples from all PBDs and all PCollections. */
  public Map<String, List<byte[]>> allSamples() {
    return samplesFor(ImmutableSet.of(), ImmutableSet.of());
  }

  /**
   * @param descriptors PBDs to filter on.
   * @return samples only from the given descriptors.
   */
  public Map<String, List<byte[]>> samplesForDescriptors(Set<String> descriptors) {
    return samplesFor(descriptors, ImmutableSet.of());
  }

  /**
   * @param pcollections PCollection ids to filter on.
   * @return samples only from the given PCollections.
   */
  public Map<String, List<byte[]>> samplesForPCollections(Set<String> pcollections) {
    return samplesFor(ImmutableSet.of(), pcollections);
  }
}
