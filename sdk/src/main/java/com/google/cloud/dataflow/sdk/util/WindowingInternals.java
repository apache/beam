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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface that may be required by some (internal) {@code DoFn}s to implement windowing.
 *
 * <p>This interface should be provided by runner implementors to support windowing on their runner.
 *
 * @param <I> input type
 * @param <O> output type
 */
public interface WindowingInternals<I, O> {

  /**
   * Output the value at the specified timestamp in the listed windows.
   */
  void outputWindowedValue(O output, Instant timestamp,
      Collection<? extends BoundedWindow> windows);

  /**
   * Writes the provided value to the list of values in stored state corresponding to the
   * provided tag.
   *
   * @throws IOException if encoding the given value fails
   */
  <T> void writeToTagList(CodedTupleTag<T> tag, T value, Instant timestamp) throws IOException;

  /**
   * Deletes the list corresponding to the given tag.
   */
  <T> void deleteTagList(CodedTupleTag<T> tag);

  /**
   * Reads the elements of the list in stored state corresponding to the provided tag.
   * If the tag is undefined, will return an empty list rather than null.
   *
   * @throws IOException if decoding any of the requested values fails
   */
  <T> Iterable<TimestampedValue<T>> readTagList(CodedTupleTag<T> tag) throws IOException;

  /**
   * Writes out a timer to be fired when the watermark reaches the given
   * timestamp.  Timers are identified by their name, and can be moved
   * by calling {@code setTimer} again, or deleted with {@link #deleteTimer}.
   */
  void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain);

  /**
   * Deletes the given timer.
   */
  void deleteTimer(String timer, Trigger.TimeDomain domain);

  /**
   * Access the windows the element is being processed in without "exploding" it.
   */
  Collection<? extends BoundedWindow> windows();

  /**
   * Write the given {@link PCollectionView} data to a location accessible by other workers.
   */
  <T> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<WindowedValue<T>> data,
      Coder<T> elemCoder) throws IOException;
}
