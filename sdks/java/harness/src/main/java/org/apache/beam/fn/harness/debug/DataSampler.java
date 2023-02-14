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
import java.util.List;
import java.util.Map;
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

  /**
   * Creates a DataSampler to sample every 1000 elements while keeping a maximum of 10 in memory.
   */
  public DataSampler() {
    this.maxSamples = 10;
    this.sampleEveryN = 1000;
  }

  /**
   * @param maxSamples Sets the maximum number of samples held in memory at once.
   * @param sampleEveryN Sets how often to sample.
   */
  public DataSampler(int maxSamples, int sampleEveryN) {
    this.maxSamples = maxSamples;
    this.sampleEveryN = sampleEveryN;
  }

  // Maximum number of elements in buffer.
  private final int maxSamples;

  // Sampling rate.
  private final int sampleEveryN;

  // The fully-qualified type is: Map[PCollectionId, OutputSampler]. In order to sample
  // on a PCollection-basis and not per-bundle, this keeps track of shared samples between states.
  private final Map<String, OutputSampler<?>> outputSamplers = new ConcurrentHashMap<>();

  /**
   * Creates and returns a class to sample the given PCollection in the given
   * ProcessBundleDescriptor. Uses the given coder encode samples as bytes when responding to a
   * SampleDataRequest.
   *
   * @param pcollectionId The PCollection to take intermittent samples from.
   * @param coder The coder associated with the PCollection. Coder may be from a nested context.
   * @param <T> The type of element contained in the PCollection.
   * @return the OutputSampler corresponding to the unique PBD and PCollection.
   */
  public <T> OutputSampler<T> sampleOutput(String pcollectionId, Coder<T> coder) {
    outputSamplers.putIfAbsent(
        pcollectionId, new OutputSampler<>(coder, this.maxSamples, this.sampleEveryN));
    return (OutputSampler<T>) outputSamplers.get(pcollectionId);
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
    BeamFnApi.SampleDataRequest sampleDataRequest = request.getSampleData();

    Map<String, List<byte[]>> responseSamples =
        samplesFor(sampleDataRequest.getPcollectionIdsList());

    BeamFnApi.SampleDataResponse.Builder response = BeamFnApi.SampleDataResponse.newBuilder();
    for (Map.Entry<String, List<byte[]>> entry : responseSamples.entrySet()) {
      String pcollectionId = entry.getKey();
      ElementList.Builder elementList = ElementList.newBuilder();
      for (byte[] sample : entry.getValue()) {
        elementList.addElements(
            SampledElement.newBuilder().setElement(ByteString.copyFrom(sample)).build());
      }
      response.putElementSamples(pcollectionId, elementList.build());
    }

    return BeamFnApi.InstructionResponse.newBuilder().setSampleData(response);
  }

  /**
   * Returns a map from PCollection to its samples. Samples are filtered on
   * ProcessBundleDescriptorIds and PCollections. Thread-safe.
   *
   * @param pcollections Filters all PCollections on this set. If empty, allows all PCollections.
   * @return a map from PCollection to its samples.
   */
  private Map<String, List<byte[]>> samplesFor(List<String> pcollections) {
    Map<String, List<byte[]>> samples = new HashMap<>();

    // Safe to iterate as the ConcurrentHashMap will return each element at most once and will not
    // throw
    // ConcurrentModificationException.
    outputSamplers.forEach(
        (pcollectionId, outputSampler) -> {
          if (!pcollections.isEmpty() && !pcollections.contains(pcollectionId)) {
            return;
          }

          samples.putIfAbsent(pcollectionId, new ArrayList<>());
          samples.get(pcollectionId).addAll(outputSampler.samples());
        });

    return samples;
  }
}
