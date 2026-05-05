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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for iterators that decode messages from bundles inside a {@link Windmill.WorkItem}.
 */
public abstract class WindmillReaderIteratorBase<T>
    extends NativeReader.NativeReaderIterator<WindowedValue<T>> {
  private final Windmill.WorkItem work;
  private int bundleIndex = 0;
  private int messageIndex = -1;
  private @Nullable WindowedValue<T> current = null;
  private final ValueProvider<Boolean> skipUndecodableElements;
  private static final Logger LOG = LoggerFactory.getLogger(WindmillReaderIteratorBase.class);

  protected WindmillReaderIteratorBase(
      Windmill.WorkItem work, ValueProvider<Boolean> skipUndecodableElements) {
    this.skipUndecodableElements = skipUndecodableElements;
    this.work = work;
  }

  @Override
  public boolean start() throws IOException {
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    while (true) {
      if (bundleIndex >= work.getMessageBundlesCount()) {
        current = null;
        return false;
      }
      Windmill.InputMessageBundle bundle = work.getMessageBundles(bundleIndex);
      ++messageIndex;
      if (messageIndex >= bundle.getMessagesCount()) {
        messageIndex = -1;
        ++bundleIndex;
        continue;
      }
      try {
        current = checkNotNull(decodeMessage(bundle.getMessages(messageIndex)));
        return true;
      } catch (RuntimeException | IOException e) {
        if (skipUndecodableElements.isAccessible()
            && Boolean.TRUE.equals(skipUndecodableElements.get())) {
          LOG.error(
              "Skipping input element for work token {} on sharding key {} due to decoding error",
              work.getWorkToken(),
              work.getShardingKey(),
              e);
          continue;
        }
        throw e;
      }
    }
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
