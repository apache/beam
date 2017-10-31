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
package org.apache.beam.sdk.extensions.sketching.frequency;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PTransform}s for finding the k most frequent elements in a {@code PCollection}, or
 * the k most frequent values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>This class uses the Space-Saving algorithm, introduced in this paper :
 * <a>https://pdfs.semanticscholar.org/72f1/5aba2e67b1cc9cd1fb12c99e101c4c1aae4b.pdf</a>
 * <br>The implementation comes from Addthis' library Stream-lib : <a>https://github.com/addthis/stream-lib</a>
 */
public class KMostFrequent {

  private static final Logger LOG = LoggerFactory.getLogger(KMostFrequent.class);

  // do not instantiate
  private KMostFrequent() {
  }

  /**
   * A {@code PTransform} that takes a {@code PCollection<T>} and returns a
   * {@code PCollection<StreamSummary<T>>} whose contents is a sketch which contains
   * the most frequent elements in the input {@code PCollection}.
   *
   * <p>The {@code capacity} parameter controls the maximum number of elements the sketch
   * can contain. Once this capacity is reached the least frequent element is dropped each
   * time an incoming element is not already present in the sketch.
   * Each element in the sketch is associated to a counter, that keeps track of the estimated
   * frequency as well as the maximal potential error.
   * <br>See {@link KMostFrequentFn} for more details.
   *
   * <p>Example of use
   * <pre>{@code PCollection<String> input = ...;
   * PCollection<StreamSummary<String>> ssSketch = input
   *        .apply(KMostFrequent.<String>perKey(10000));
   * }</pre>
   *
   * @param capacity    the maximum number of distinct elements that the Stream Summary can keep
   *                    track of at the same time
   * @param <T>         the type of the elements in the input {@code PCollection}
   */
  public static <T> Combine.Globally<T, StreamSummary<T>> globally(int capacity) {
    return Combine.<T, StreamSummary<T>>globally(KMostFrequentFn.<T>create(capacity));
  }

  /**
   * A {@code PTransform} that takes an input {@code PCollection<KV<K, T>>} and returns a
   * {@code PCollection<KV<K, StreamSummary<T>>} that contains an output element mapping each
   * distinct key in the input {@code PCollection} to a sketch which contains the most frequent
   * values associated with that key in the input {@code PCollection}.
   *
   * <p>The {@code capacity} parameter controls the maximum number of elements the sketch
   * can contain. Once this capacity is reached the least frequent element is dropped each
   * time an incoming element is not already present in the sketch.
   * Each element in the sketch is associated to a counter, that keeps track of the estimated
   * frequency as well as the maximal potential error.
   * <br>See {@link KMostFrequentFn} for more details.
   *
   * <p>Example of use
   * <pre>{@code PCollection<KV<Integer, String>> input = ...;
   * PCollection<KV<Integer, StreamSummary<String>>> ssSketch = input
   *        .apply(KMostFrequent.<Integer, String>globally(10000));
   * }</pre>
   *
   * @param capacity    the maximum number of distinct elements that the Stream Summary can keep
   *                    track of at the same time
   * @param <K>         the type of the keys in the input and output {@code PCollection}s
   * @param <T>         the type of values in the input {@code PCollection}
   */
  public static <K, T> Combine.PerKey<K, T, StreamSummary<T>> perKey(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("The capacity must be strictly positive");
    }
    return Combine.<K, T, StreamSummary<T>>perKey(KMostFrequentFn.<T>create(capacity));
  }

  /**
   * A {@code Combine.CombineFn} that computes the stream into a {@link StreamSummary}
   * sketch, useful as an argument to {@link Combine#globally} or {@link Combine#perKey}.
   *
   * <p>The Space-Saving algorithm summarizes the stream by using a doubly linked-list of buckets
   * ordered by the frequency value they represent. Each of these buckets contains a linked-list
   * of counters which estimate the {@code count} for an element as well as the maximum
   * overestimation {@code e} associated to it. The frequency cannot be overestimated.
   *
   * <p>An element is guaranteed to be in the top K most frequent if its guaranteed number of hits,
   * i.e. {@code count - e}, is greater than the count of the element at the position k+1.
   *
   * @param <T>         the type of the elements being combined
   */
  public static class KMostFrequentFn<T>
          extends Combine.CombineFn<T, StreamSummary<T>, StreamSummary<T>> {

    private int capacity;

    private KMostFrequentFn(int capacity) {
      this.capacity = capacity;
    }

    public static <T> KMostFrequentFn<T> create(int capacity) {
      if (capacity <= 0) {
        throw new IllegalArgumentException("Capacity must be greater than 0.");
      }
      return new KMostFrequentFn<>(capacity);
    }

    @Override
    public StreamSummary<T> createAccumulator() {
      return new StreamSummary<>(this.capacity);
    }

    @Override
    public StreamSummary<T> addInput(StreamSummary<T> accumulator, T element) {
      accumulator.offer(element, 1);
      return accumulator;
    }

    @Override
    public StreamSummary<T> mergeAccumulators(
            Iterable<StreamSummary<T>> accumulators) {
      Iterator<StreamSummary<T>> it = accumulators.iterator();
      if (it.hasNext()) {
        StreamSummary<T> mergedAccum = it.next();
        while (it.hasNext()) {
          StreamSummary<T> other = it.next();
          List<Counter<T>> top = other.topK(capacity);
          for (Counter<T> counter : top) {
            mergedAccum.offer(counter.getItem(), (int) counter.getCount());
          }
        }
        return mergedAccum;
      }
      return null;
    }

    @Override
    public StreamSummary<T> extractOutput(StreamSummary<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<StreamSummary<T>> getAccumulatorCoder(CoderRegistry registry,
                                                            Coder inputCoder) {
      return new StreamSummaryCoder<>();
    }

    @Override
    public Coder<StreamSummary<T>> getDefaultOutputCoder(CoderRegistry registry,
                                                              Coder inputCoder) {
      return new StreamSummaryCoder<>();
    }
  }

  static class StreamSummaryCoder<T> extends CustomCoder<StreamSummary<T>> {

    private static final Coder<byte[]> BYTE_ARRAY_CODER = ByteArrayCoder.of();

    @Override
    public void encode(StreamSummary<T> value, OutputStream outStream) throws IOException {
      BYTE_ARRAY_CODER.encode(value.toBytes(), outStream);
    }

    @Override
    public StreamSummary<T> decode(InputStream inStream) throws IOException {
      try {
        return new StreamSummary<>(BYTE_ARRAY_CODER.decode(inStream));
      } catch (ClassNotFoundException e) {
        LOG.error(e.getMessage()
                + " The Stream Summary sketch can't be decoded from the input stream", e);
      }
      return null;
    }
  }
}
