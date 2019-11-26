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
package org.apache.beam.runners.dataflow.worker;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;

/**
 * Base class for iterators that decode messages from bundles inside a {@link Windmill.WorkItem}.
 */
public abstract class WindmillReaderIteratorBase<T>
    extends NativeReader.NativeReaderIterator<WindowedValue<T>> {
  private Windmill.WorkItem work;
  private int bundleIndex = 0;
  private int messageIndex = -1;
  private Optional<WindowedValue<T>> current;

  protected WindmillReaderIteratorBase(Windmill.WorkItem work) {
    this.work = work;
  }

  @Override
  public boolean start() throws IOException {
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    if (bundleIndex == work.getMessageBundlesCount()
        || messageIndex == work.getMessageBundles(bundleIndex).getMessagesCount()) {
      current = Optional.absent();
      return false;
    }
    ++messageIndex;
    for (; bundleIndex < work.getMessageBundlesCount(); ++bundleIndex, messageIndex = 0) {
      Windmill.InputMessageBundle bundle = work.getMessageBundles(bundleIndex);
      if (messageIndex < bundle.getMessagesCount()) {
        current = Optional.of(decodeMessage(bundle.getMessages(messageIndex)));
        return true;
      }
    }
    current = Optional.absent();
    return false;
  }

  protected abstract WindowedValue<T> decodeMessage(Windmill.Message message) throws IOException;

  @Override
  public WindowedValue<T> getCurrent() throws NoSuchElementException {
    if (!current.isPresent()) {
      throw new NoSuchElementException();
    }
    return current.get();
  }
}
