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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ForwardingFuture;

/**
 * A future which will trigger a GetData request to Windmill for all outstanding futures on the
 * first {@link #get}.
 */
public class WrappedFuture<T> extends ForwardingFuture.SimpleForwardingFuture<T> {
  /**
   * The reader we'll use to service the eventual read. Null if read has been fulfilled.
   *
   * <p>NOTE: We must clear this after the read is fulfilled to prevent space leaks.
   */
  private @Nullable WindmillStateReader reader;

  public WrappedFuture(WindmillStateReader reader, Future<T> delegate) {
    super(delegate);
    this.reader = reader;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    if (!delegate().isDone() && reader != null) {
      // Only one thread per reader, so no race here.
      reader.performReads();
    }
    reader = null;
    return super.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (!delegate().isDone() && reader != null) {
      // Only one thread per reader, so no race here.
      reader.performReads();
    }
    reader = null;
    return super.get(timeout, unit);
  }
}
