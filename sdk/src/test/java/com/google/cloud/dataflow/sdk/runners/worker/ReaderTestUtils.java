/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toCloudPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toDynamicSplitRequest;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.ConcatPosition;
import com.google.api.services.dataflow.model.Position;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader.ReaderIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Helpers for testing {@code Reader} and related classes, especially
 * {@link Reader.ReaderIterator#getProgress} and {@link Reader.ReaderIterator#requestDynamicSplit}.
 */
public class ReaderTestUtils {
  public static Position positionAtIndex(@Nullable Long index) {
    return new Position().setRecordIndex(index);
  }

  public static Position positionAtByteOffset(@Nullable Long byteOffset) {
    return new Position().setByteOffset(byteOffset);
  }

  public static Position positionAtConcatPosition(
      @Nullable Integer index, @Nullable Position innerPosition) {
    return new Position().setConcatPosition(
        new ConcatPosition().setIndex(index).setPosition(innerPosition));
  }

  public static ApproximateProgress approximateProgressAtPosition(@Nullable Position position) {
    return new ApproximateProgress().setPosition(position);
  }

  public static ApproximateProgress approximateProgressAtIndex(@Nullable Long index) {
    return approximateProgressAtPosition(positionAtIndex(index));
  }

  public static ApproximateProgress approximateProgressAtByteOffset(@Nullable Long byteOffset) {
    return approximateProgressAtPosition(positionAtByteOffset(byteOffset));
  }

  public static ApproximateProgress approximateProgressAtConcatPosition(
      @Nullable Integer index, @Nullable Position innerPosition) {
    return approximateProgressAtPosition(positionAtConcatPosition(index, innerPosition));
  }

  public static ApproximateProgress approximateProgressAtFraction(@Nullable Float fraction) {
    return new ApproximateProgress().setPercentComplete(fraction);
  }

  public static Reader.DynamicSplitRequest splitRequestAtPosition(@Nullable Position position) {
    return toDynamicSplitRequest(approximateProgressAtPosition(position));
  }

  public static Reader.DynamicSplitRequest splitRequestAtIndex(@Nullable Long index) {
    return toDynamicSplitRequest(approximateProgressAtIndex(index));
  }

  public static Reader.DynamicSplitRequest splitRequestAtByteOffset(@Nullable Long byteOffset) {
    return toDynamicSplitRequest(approximateProgressAtByteOffset(byteOffset));
  }

  public static Reader.DynamicSplitRequest splitRequestAtConcatPosition(
      @Nullable Integer index, @Nullable Position innerPosition) {
    return toDynamicSplitRequest(approximateProgressAtConcatPosition(index, innerPosition));
  }

  public static Position positionFromSplitResult(Reader.DynamicSplitResult dynamicSplitResult) {
    return toCloudPosition(
        ((Reader.DynamicSplitResultWithPosition) dynamicSplitResult).getAcceptedPosition());
  }

  public static Position positionFromProgress(Reader.Progress progress) {
    return readerProgressToCloudProgress(progress).getPosition();
  }

  public static Reader.DynamicSplitRequest splitRequestAtFraction(float fraction) {
    return toDynamicSplitRequest(approximateProgressAtFraction(fraction));
  }

  /**
   * Creates a {@link ReaderIterator} from the given {@code Reader} and reads it to the end.
   *
   * @param reader {@code Reader} to read from
   * @param results elements that are read are added to this list. Will contain partially read
   * results if an exception is thrown
   * @throws IOException
   */
  public static <T> void readRemainingFromReader(Reader<T> reader, List<T> results)
      throws IOException {
    try (ReaderIterator<T> iterator = reader.iterator()) {
      readRemainingFromIterator(iterator, results);
    }
  }

  /**
   * Read elements from a {@code Reader} until either the reader is exhausted, or n elements are
   * read.
   */
  public static <T> void readAtMostNElementsFromReader(
      Reader<T> reader, long numElementsToRead, List<T> results) throws IOException {
    try (ReaderIterator<T> iterator = reader.iterator()) {
      readAtMostNElementsFromIterator(iterator, numElementsToRead, results);
    }
  }

  /**
   * Read elements from a {@link ReaderIterator} until either the iterator is exhausted, or n
   * elements are read.
   */
  public static <T> void readAtMostNElementsFromIterator(
      ReaderIterator<T> iterator, long numElementsToRead, List<T> results) throws IOException {
    for (long i = 0L; i < numElementsToRead && iterator.hasNext(); i++) {
      results.add(iterator.next());
    }
  }

  /**
   * Read all remaining elements from a {@link ReaderIterator}.
   */
  public static <T> void readRemainingFromIterator(ReaderIterator<T> iterator, List<T> results)
      throws IOException {
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }
  }

  /**
   * Appends all values from a collection of {@code WindowedValue} values to a collection of values.
   */
  public static <T> void windowedValuesToValues(
      Collection<WindowedValue<T>> windowedValues, Collection<T> values) {
    for (WindowedValue<T> windowedValue : windowedValues) {
      values.add(windowedValue.getValue());
    }
  }
}
