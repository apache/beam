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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds samples for a single PCollection until queried by the parent DataSampler. This
 * class is meant to hold only a limited number of elements in memory. So old values are constantly
 * being overridden in a circular buffer.
 *
 * @param <T> the element type of the PCollection.
 */
public class OutputSampler<T> {
  private final Coder<T> coder;
  private static final Logger LOG = LoggerFactory.getLogger(OutputSampler.class);

  // Temporarily holds elements until the SDK receives a sample data request.
  private final List<T> buffer;

  // Maximum number of elements in buffer.
  private final int maxElements;

  // Sampling rate.
  private final int sampleEveryN;

  // Total number of samples taken.
  private long numSamples = 0;

  // Index into the buffer of where to overwrite samples.
  private int resampleIndex = 0;

  public OutputSampler(Coder<T> coder, int maxElements, int sampleEveryN) {
    this.coder = coder;
    this.maxElements = maxElements;
    this.sampleEveryN = sampleEveryN;
    this.buffer = new ArrayList<>(this.maxElements);
  }

  /**
   * Samples every 1000th element or if it is part of the first 10 in the (local) PCollection.
   *
   * @param element the element to sample.
   */
  public synchronized void sample(T element) {
    // Only sample the first 10 elements then after every `sampleEveryN`th element.
    numSamples += 1;
    if (numSamples > 10 && numSamples % sampleEveryN != 0) {
      return;
    }

    // Fill buffer until maxElements.
    if (buffer.size() < maxElements) {
      buffer.add(element);
    } else {
      // Then rewrite sampled elements as a circular buffer.
      buffer.set(resampleIndex, element);
      resampleIndex = (resampleIndex + 1) % maxElements;
    }
  }

  /**
   * Clears samples at end of call. This is to help mitigate memory use.
   *
   * @return samples taken since last call.
   */
  public List<byte[]> samples() {
    List<byte[]> ret = new ArrayList<>();

    // Serializing can take a lot of CPU time for larger or complex elements. Copy the array here
    // so as to not slow down the main processing hot path.
    List<T> copiedBuffer;
    synchronized (this) {
      copiedBuffer = new ArrayList<>(buffer);
      clear();
    }

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    for (T el : copiedBuffer) {
      try {
        // This is deprecated, but until this is fully removed, this specifically needs the nested
        // context. This is because the SDK will need to decode the sampled elements with the
        // ToStringFn.
        coder.encode(el, stream, Coder.Context.NESTED);
        ret.add(stream.toByteArray());
      } catch (Exception exception) {
        LOG.warn("Could not encode element \"" + el + "\" to bytes: " + exception);
      } finally {
        stream.reset();
      }
    }

    return ret;
  }

  private void clear() {
    buffer.clear();
    resampleIndex = 0;
  }
}
