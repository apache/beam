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

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for buffering outputs.
 *
 * @param <K> The type of key being held by the buffer.
 * @param <InputT> The type of values added to the buffer.
 * @param <OutputT> The type of values extracted from the buffer.
 * @param <W> The type of windows being buffered.
 */
public interface OutputBuffer<K, InputT, OutputT, W extends BoundedWindow>
    extends Serializable {

  public static final String BUFFER_NAME = "__buffer";

  /**
   * Context methods necessary for implementing {@code OutputBuffer}s.
   *
   * <p> Provides access to underlying (runner specific) storage for buffering values.
   *
   * @param <K> The type of key being held by the buffer.
   * @param <W> The type of windows being buffered.
   */
  public interface Context<K, W extends BoundedWindow> {
    K key();
    W window();
    Iterable<W> sourceWindows();

    <T> void addToBuffer(
        W window, CodedTupleTag<T> buffer, T value) throws IOException;
    <T> void addToBuffer(
        W window, CodedTupleTag<T> buffer, T value, Instant timestamp) throws IOException;
    void clearBuffers(CodedTupleTag<?> buffer, Iterable<W> windows) throws IOException;
    <T> Iterable<T> readBuffers(CodedTupleTag<T> buffer, Iterable<W> windows) throws IOException;
  }

  /**
   * Add a value to this buffer.
   */
  void addValue(Context<K, W> c, InputT input) throws IOException;

  /**
   * Extract the output value from the {@code OutputBuffer}.
   */
  OutputT extract(Context<K, W> c) throws IOException;

  /**
   * Clear the contents of the output buffer.
   */
  void clear(Context<K, W> c) throws IOException;

  /**
   * Flush any buffered state.
   */
  void flush(Context<K, W> c) throws IOException;
}
