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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A class that safely ensures an object of type T is cleaned up before it is garbage collected. */
class CloserReference<T extends AutoCloseable> implements Supplier<T> {

  private static final Logger LOG = LoggerFactory.getLogger(CloserReference.class);

  private final T object;

  public static <T extends AutoCloseable> CloserReference<T> of(T object) {
    return new CloserReference<>(object);
  }

  @Override
  public T get() {
    return object;
  }

  private CloserReference(T object) {
    this.object = object;
  }

  private static class Closer implements Runnable {

    private final AutoCloseable object;

    private Closer(AutoCloseable object) {
      this.object = object;
    }

    @Override
    public void run() {
      try {
        object.close();
      } catch (Exception e) {
        LOG.warn("Failed to close resource with class: " + object.getClass().getCanonicalName(), e);
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void finalize() {
    SystemExecutors.getFuturesExecutor().execute(new Closer(object));
  }
}
