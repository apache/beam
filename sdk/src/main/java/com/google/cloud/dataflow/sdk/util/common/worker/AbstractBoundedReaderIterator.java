/*******************************************************************************
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
 ******************************************************************************/
package com.google.cloud.dataflow.sdk.util.common.worker;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * An abstract base class for implementations of ReaderIterator classes for bounded sources,
 * where hasNext() returns the same value if called multiple times.
 *
 * <p>Provides basic treatment of hasNext()/next() to simplify implementations (e.g. ensuring
 * hasNext() is called only once and verifying hasNext() in next()) and default no-op
 * implementations of other operations.
 *
 * <p><i>This class is intended for internal usage. Users of Dataflow must not subclass it.</i>
 *
 * @param <T> Type of records returned by the reader.
 */
public abstract class AbstractBoundedReaderIterator<T> extends Reader.AbstractReaderIterator<T> {
  private Boolean cachedHasNext;

  @Override
  public final boolean hasNext() throws IOException {
    if (cachedHasNext == null) {
      cachedHasNext = hasNextImpl();
    }
    return cachedHasNext;
  }

  protected abstract boolean hasNextImpl() throws IOException;

  @Override
  public final T next() throws IOException, NoSuchElementException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    cachedHasNext = null;
    return nextImpl();
  }

  protected abstract T nextImpl()  throws IOException;
}
