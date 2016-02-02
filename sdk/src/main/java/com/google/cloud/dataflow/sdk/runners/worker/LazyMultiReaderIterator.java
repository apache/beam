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

import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implements a ReaderIterator over a collection of inputs.
 *
 * <p>The sources are used sequentially, each consumed entirely before moving
 * to the next source.
 *
 * <p>The input is lazily constructed by using the abstract method {@code open}
 * to create a source iterator for inputs on demand.  This allows the resources
 * to be produced lazily, as an open source iterator may consume process
 * resources such as file descriptors.
 */
abstract class LazyMultiReaderIterator<T> extends NativeReader.NativeReaderIterator<T> {
  private final Iterator<String> inputs;
  private NativeReader.NativeReaderIterator<T> current;

  public LazyMultiReaderIterator(Iterator<String> inputs) {
    this.inputs = inputs;
  }

  @Override
  public boolean start() throws IOException {
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    boolean currentStarted = true;
    while (true) {
      // Try moving through the current reader
      if (current != null) {
        if (currentStarted ? current.advance() : current.start()) {
          return true;
        }
        current.close();
        current = null;
      }
      // Current reader is done - move on to the next one.
      if (!inputs.hasNext()) {
        return false;
      }
      current = open(inputs.next());
      currentStarted = false;
    }
  }

  @Override
  public T getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current.getCurrent();
  }

  @Override
  public void close() throws IOException {
    if (current != null) {
      current.close();
      current = null;
    }
  }

  protected abstract NativeReader.NativeReaderIterator<T> open(String input) throws IOException;
}
