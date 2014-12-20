/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implements a ReaderIterator over a collection of inputs.
 *
 * The sources are used sequentially, each consumed entirely before moving
 * to the next source.
 *
 * The input is lazily constructed by using the abstract method {@code open} to
 * create a source iterator for inputs on demand.  This allows the resources to
 * be produced lazily, as an open source iterator may consume process resources
 * such as file descriptors.
 */
abstract class LazyMultiReaderIterator<T> extends Reader.AbstractReaderIterator<T> {
  private final Iterator<String> inputs;
  Reader.ReaderIterator<T> current;

  public LazyMultiReaderIterator(Iterator<String> inputs) {
    this.inputs = inputs;
  }

  @Override
  public boolean hasNext() throws IOException {
    while (selectReader()) {
      if (!current.hasNext()) {
        current.close();
        current = null;
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public T next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return current.next();
  }

  @Override
  public void close() throws IOException {
    while (selectReader()) {
      current.close();
      current = null;
    }
  }

  protected abstract Reader.ReaderIterator<T> open(String input) throws IOException;

  boolean selectReader() throws IOException {
    if (current != null) {
      return true;
    }
    if (inputs.hasNext()) {
      current = open(inputs.next());
      return true;
    }
    return false;
  }
}
