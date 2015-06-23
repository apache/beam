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
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.collect.Iterables;

import java.io.IOException;

/**
 * {code OutputBuffer} that buffers input values emitting all added values as an Iterable.
 */
class ListOutputBuffer<K, T, W extends BoundedWindow>
    implements OutputBuffer<K, T, Iterable<T>, W> {

  private static final long serialVersionUID = 0L;

  private CodedTupleTag<T> bufferTag;

  public ListOutputBuffer(Coder<T> itemCoder) {
    bufferTag = CodedTupleTag.of(BUFFER_NAME, itemCoder);
  }

  @Override
  public void addValue(Context<K, W> c, T input) throws IOException {
    c.addToBuffer(c.window(), bufferTag, input);
  }

  @Override
  public Iterable<T> extract(Context<K, W> c) throws IOException {
    Iterable<T> result = c.readBuffers(bufferTag, c.sourceWindows());
    return Iterables.isEmpty(result) ? null : result;
  }

  @Override
  public void clear(Context<K, W> c) throws IOException {
    c.clearBuffers(bufferTag, c.sourceWindows());
  }

  @Override
  public void flush(Context<K, W> c) throws IOException {
  }
}
