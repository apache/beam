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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * This class holds samples for a single PCollection until queried by the parent DataSampler. This
 * class is meant to hold only a limited number of elements in memory. So old values are constantly
 * being overridden in a circular buffer.
 *
 * @param <T> the element type of the PCollection.
 */
public class OutputSampler<T> {

  // Temporarily holds elements until the SDK receives a sample data request.
  private List<WindowedValue<T>> buffer;

  // Maximum number of elements in buffer.
  private final int maxElements;

  // Sampling rate.
  private final int sampleEveryN;

  // Total number of samples taken.
  private final AtomicLong numSamples = new AtomicLong();

  // Index into the buffer of where to overwrite samples.
  private int resampleIndex = 0;

  @Nullable private final Coder<T> valueCoder;

  @Nullable private final Coder<WindowedValue<T>> windowedValueCoder;

  public OutputSampler(Coder<?> coder, int maxElements, int sampleEveryN) {
    this.maxElements = maxElements;
    this.sampleEveryN = sampleEveryN;
    this.buffer = new ArrayList<>(this.maxElements);

    // The samples taken and encoded should match exactly to the specification from the
    // ProcessBundleDescriptor. The coder given can either be a WindowedValueCoder, in which the
    // element itself is sampled. Or, it's non a WindowedValueCoder and the value inside the
    // windowed value must be sampled. This is because WindowedValue is the element type used in
    // all receivers, which doesn't necessarily match the PBD encoding.
    if (coder instanceof WindowedValue.WindowedValueCoder) {
      this.valueCoder = null;
      this.windowedValueCoder = (Coder<WindowedValue<T>>) coder;
    } else {
      this.valueCoder = (Coder<T>) coder;
      this.windowedValueCoder = null;
    }
  }

  /**
   * Samples every {@code sampleEveryN}th element or if it is part of the first 10 in the (local)
   * PCollection.
   *
   * <p>This method is invoked in parallel by multiple bundle processing threads and in parallel to
   * any {@link #samples} being returned to a thread handling a sample request.
   *
   * @param element the element to sample.
   */
  public void sample(WindowedValue<T> element) {
    // Only sample the first 10 elements then after every `sampleEveryN`th element.
    long samples = numSamples.get() + 1;

    // This has eventual consistency. If there are many threads lazy setting, this will be set to
    // the slowest thread accessing the atomic. But over time, it will still increase. This is ok
    // because this is a debugging feature and doesn't need strict atomics.
    numSamples.lazySet(samples);
    if (samples > 10 && samples % sampleEveryN != 0) {
      return;
    }

    synchronized (this) {
      // Fill buffer until maxElements.
      if (buffer.size() < maxElements) {
        buffer.add(element);
      } else {
        // Then rewrite sampled elements as a circular buffer.
        buffer.set(resampleIndex, element);
        resampleIndex = (resampleIndex + 1) % maxElements;
      }
    }
  }

  /**
   * Clears samples at end of call. This is to help mitigate memory use.
   *
   * <p>This method is invoked by a thread handling a data sampling request in parallel to any calls
   * to {@link #sample}.
   *
   * @return samples taken since last call.
   */
  public List<BeamFnApi.SampledElement> samples() throws IOException {
    List<BeamFnApi.SampledElement> ret = new ArrayList<>();

    // Serializing can take a lot of CPU time for larger or complex elements. Copy the array here
    // so as to not slow down the main processing hot path.
    List<WindowedValue<T>> bufferToSend;
    int sampleIndex = 0;
    synchronized (this) {
      bufferToSend = buffer;
      sampleIndex = resampleIndex;
      buffer = new ArrayList<>(maxElements);
      resampleIndex = 0;
    }

    ByteStringOutputStream stream = new ByteStringOutputStream();
    for (int i = 0; i < bufferToSend.size(); i++) {
      int index = (sampleIndex + i) % bufferToSend.size();

      if (valueCoder != null) {
        this.valueCoder.encode(bufferToSend.get(index).getValue(), stream, Coder.Context.NESTED);
      } else if (windowedValueCoder != null) {
        this.windowedValueCoder.encode(bufferToSend.get(index), stream, Coder.Context.NESTED);
      }

      ret.add(
          BeamFnApi.SampledElement.newBuilder().setElement(stream.toByteStringAndReset()).build());
    }

    return ret;
  }
}
