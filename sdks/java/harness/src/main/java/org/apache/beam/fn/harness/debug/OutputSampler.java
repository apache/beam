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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
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
  private List<ElementSample<T>> buffer;

  // Temporarily holds exceptional elements. These elements can also be duplicated in the main
  // buffer. This is in order to always track exceptional elements even if the number of samples in
  // the main buffer drops it.
  private Map<String, ElementSample<T>> exceptions = new HashMap<>();

  // Maximum number of elements in buffer.
  private final int maxElements;

  // Sampling rate.
  private final int sampleEveryN;

  // Total number of samples taken.
  private final AtomicLong numSamples = new AtomicLong();

  // Index into the buffer of where to overwrite samples.
  private int resampleIndex = 0;

  // If true, only takes samples when exceptions in UDFs occur.
  private final Boolean onlySampleExceptions;

  @Nullable private final Coder<T> valueCoder;

  @Nullable private final Coder<WindowedValue<T>> windowedValueCoder;

  public OutputSampler(
      Coder<?> coder, int maxElements, int sampleEveryN, boolean onlySampleExceptions) {
    this.maxElements = maxElements;
    this.sampleEveryN = sampleEveryN;
    this.buffer = new ArrayList<>(this.maxElements);
    this.onlySampleExceptions = onlySampleExceptions;

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
  public ElementSample<T> sample(WindowedValue<T> element) {
    // Only sample the first 10 elements then after every `sampleEveryN`th element.
    long samples = numSamples.get() + 1;

    // This has eventual consistency. If there are many threads lazy setting, this will be set to
    // the slowest thread accessing the atomic. But over time, it will still increase. This is ok
    // because this is a debugging feature and doesn't need strict atomics.
    numSamples.lazySet(samples);

    ElementSample<T> elementSample =
        new ElementSample<>(ThreadLocalRandom.current().nextInt(), element);
    if (onlySampleExceptions || (samples > 10 && samples % sampleEveryN != 0)) {
      return elementSample;
    }

    synchronized (this) {
      // Fill buffer until maxElements.
      if (buffer.size() < maxElements) {
        buffer.add(elementSample);
      } else {
        // Then rewrite sampled elements as a circular buffer.
        buffer.set(resampleIndex, elementSample);
        resampleIndex = (resampleIndex + 1) % maxElements;
      }
    }

    return elementSample;
  }

  /**
   * Samples an exceptional element to be later queried. The enforces that only one exception occurs
   * per bundle.
   *
   * @param elementSample the sampled element to add an exception to.
   * @param e the exception.
   * @param ptransformId the source of the exception.
   * @param processBundleId the failing bundle.
   */
  public void exception(
      ElementSample<T> elementSample, Exception e, String ptransformId, String processBundleId) {
    if (elementSample == null || processBundleId == null) {
      return;
    }

    synchronized (this) {
      exceptions.computeIfAbsent(
          processBundleId,
          pbId -> {
            elementSample.exception =
                new ElementSample.ExceptionMetadata(e.toString(), ptransformId);
            return elementSample;
          });
    }
  }

  /**
   * Fills and returns the BeamFnApi proto.
   *
   * @param sample the sampled element.
   * @param stream the stream to use to serialize the element.
   * @param processBundleId the bundle the element belongs to. Currently only set when there is an
   *     exception.
   */
  private BeamFnApi.SampledElement sampleToProto(
      ElementSample<T> sample, ByteStringOutputStream stream, @Nullable String processBundleId)
      throws IOException {
    if (valueCoder != null) {
      this.valueCoder.encode(sample.sample.getValue(), stream, Coder.Context.NESTED);
    } else if (windowedValueCoder != null) {
      this.windowedValueCoder.encode(sample.sample, stream, Coder.Context.NESTED);
    }

    BeamFnApi.SampledElement.Builder elementBuilder =
        BeamFnApi.SampledElement.newBuilder().setElement(stream.toByteStringAndReset());

    ElementSample.ExceptionMetadata exception = sample.exception;
    if (exception != null) {
      BeamFnApi.SampledElement.Exception.Builder exceptionBuilder =
          BeamFnApi.SampledElement.Exception.newBuilder()
              .setTransformId(exception.ptransformId)
              .setError(exception.message);

      if (processBundleId != null) {
        exceptionBuilder.setInstructionId(processBundleId);
      }

      elementBuilder.setException(exceptionBuilder);
    }

    return elementBuilder.build();
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
    List<ElementSample<T>> bufferToSend;
    Map<String, ElementSample<T>> exceptionsToSend;
    int sampleIndex = 0;
    synchronized (this) {
      bufferToSend = buffer;
      buffer = new ArrayList<>(maxElements);

      exceptionsToSend = exceptions;
      exceptions = new HashMap<>(exceptions.size());

      sampleIndex = resampleIndex;
      resampleIndex = 0;
    }

    // An element can live in both the main samples and exception buffer. Use a small look up table
    // to deduplicate samples.
    HashSet<Long> seen = new HashSet<>();
    ByteStringOutputStream stream = new ByteStringOutputStream();
    for (Map.Entry<String, ElementSample<T>> pair : exceptionsToSend.entrySet()) {
      String processBundleId = pair.getKey();
      ElementSample<T> sample = pair.getValue();
      seen.add(sample.id);

      ret.add(sampleToProto(sample, stream, processBundleId));
    }

    for (int i = 0; i < bufferToSend.size(); i++) {
      int index = (sampleIndex + i) % bufferToSend.size();

      ElementSample<T> sample = bufferToSend.get(index);
      if (seen.contains(sample.id)) {
        continue;
      }

      ret.add(sampleToProto(sample, stream, null));
    }

    return ret;
  }
}
