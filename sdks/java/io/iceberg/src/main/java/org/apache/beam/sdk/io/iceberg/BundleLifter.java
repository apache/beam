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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PTransform that buffers elements and outputs them to one of two TupleTags based on the total
 * size of the bundle in finish_bundle.
 *
 * @param <T> The type of elements in the input PCollection.
 */
public class BundleLifter<T> extends PTransform<PCollection<T>, PCollectionTuple> {

  final TupleTag<T> smallBatchTag;
  final TupleTag<T> largeBatchTag;
  final int threshold;
  final SerializableFunction<T, Integer> elementSizer;

  /**
   * A private, static DoFn that buffers elements within a bundle and outputs them to different tags
   * in finish_bundle based on the total bundle size.
   *
   * @param <T> The type of elements being processed.
   */
  private static class BundleLiftDoFn<T> extends DoFn<T, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(BundleLiftDoFn.class);

    final TupleTag<T> smallBatchTag;
    final TupleTag<T> largeBatchTag;
    final int threshold;
    final SerializableFunction<T, Integer> elementSizer;

    private transient @MonotonicNonNull List<T> buffer;
    private transient long bundleSizeBytes;
    private transient @Nullable MultiOutputReceiver receiver;

    BundleLiftDoFn(
        TupleTag<T> smallBatchTag,
        TupleTag<T> largeBatchTag,
        int threshold,
        SerializableFunction<T, Integer> elementSizer) {
      this.smallBatchTag = smallBatchTag;
      this.largeBatchTag = largeBatchTag;
      this.threshold = threshold;
      this.elementSizer = elementSizer;
    }

    @StartBundle
    public void startBundle() {
      buffer = new ArrayList<>();
      receiver = null;
      bundleSizeBytes = 0L;
    }

    @ProcessElement
    public void processElement(@Element T element, MultiOutputReceiver mor) {
      if (receiver == null) {
        receiver = mor;
      }
      checkArgumentNotNull(buffer, "Buffer should be set by startBundle.");
      buffer.add(element);
      bundleSizeBytes += elementSizer.apply(element);
    }

    @FinishBundle
    public void finishBundle() {
      checkArgumentNotNull(buffer, "Buffer should be set by startBundle.");
      if (buffer.isEmpty()) {
        return;
      }

      // Select the target tag based on the bundle size
      TupleTag<T> targetTag;
      targetTag = (bundleSizeBytes < threshold) ? smallBatchTag : largeBatchTag;
      LOG.debug(
          "Emitting {} elements of {} estimated bytes to tag: '{}'",
          buffer.size(),
          bundleSizeBytes,
          targetTag.getId());

      checkArgumentNotNull(receiver, "Receiver should be set by startBundle.");
      OutputReceiver<T> taggedOutput = receiver.get(targetTag);

      for (T element : buffer) {
        taggedOutput.output(element);
      }
    }
  }

  private BundleLifter(TupleTag<T> smallBatchTag, TupleTag<T> largeBatchTag, int threshold) {
    this(smallBatchTag, largeBatchTag, threshold, x -> 1);
  }

  private BundleLifter(
      TupleTag<T> smallBatchTag,
      TupleTag<T> largeBatchTag,
      int threshold,
      SerializableFunction<T, Integer> elementSizer) {
    if (smallBatchTag == null || largeBatchTag == null) {
      throw new IllegalArgumentException("smallBatchTag and largeBatchTag must not be null");
    }
    if (smallBatchTag.getId().equals(largeBatchTag.getId())) {
      throw new IllegalArgumentException("smallBatchTag and largeBatchTag must be different");
    }
    if (threshold <= 0) {
      throw new IllegalArgumentException("Threshold must be a positive integer");
    }

    this.smallBatchTag = smallBatchTag;
    this.largeBatchTag = largeBatchTag;
    this.threshold = threshold;
    this.elementSizer = elementSizer;
  }

  public static <T> BundleLifter<T> of(
      TupleTag<T> smallBatchTag, TupleTag<T> largeBatchTag, int threshold) {
    return new BundleLifter<>(smallBatchTag, largeBatchTag, threshold);
  }

  public static <T> BundleLifter<T> of(
      TupleTag<T> smallBatchTag,
      TupleTag<T> largeBatchTag,
      int threshold,
      SerializableFunction<T, Integer> elementSizer) {
    return new BundleLifter<>(smallBatchTag, largeBatchTag, threshold, elementSizer);
  }

  @Override
  public PCollectionTuple expand(PCollection<T> input) {
    final TupleTag<Void> mainOutputTag = new TupleTag<Void>() {};

    return input.apply(
        "BundleLiftDoFn",
        ParDo.of(new BundleLiftDoFn<>(smallBatchTag, largeBatchTag, threshold, elementSizer))
            .withOutputTags(mainOutputTag, TupleTagList.of(smallBatchTag).and(largeBatchTag)));
  }
}
