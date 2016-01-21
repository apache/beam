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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Base class for iterators that decode messages from bundles inside a {@link Windmill.WorkItem}.
 */
public abstract class WindmillReaderIteratorBase<T>
    extends NativeReader.NativeReaderIterator<WindowedValue<T>> {
  private Windmill.WorkItem work;
  private int bundleIndex = 0;
  private int messageIndex = -1;
  private WindowedValue<T> current;

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
      return false;
    }
    ++messageIndex;
    for (; bundleIndex < work.getMessageBundlesCount(); ++bundleIndex, messageIndex = 0) {
      Windmill.InputMessageBundle bundle = work.getMessageBundles(bundleIndex);
      if (messageIndex < bundle.getMessagesCount()) {
        current = decodeMessage(bundle.getMessages(messageIndex));
        return true;
      }
    }
    current = null;
    return false;
  }

  protected abstract WindowedValue<T> decodeMessage(Windmill.Message message) throws IOException;

  @Override
  public WindowedValue<T> getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }
}
