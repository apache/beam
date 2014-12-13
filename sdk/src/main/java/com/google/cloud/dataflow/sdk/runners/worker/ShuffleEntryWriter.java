/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * ShuffleEntryWriter provides an interface for writing key/value
 * entries to a shuffle dataset.
 */
@NotThreadSafe
interface ShuffleEntryWriter extends AutoCloseable {
  /**
   * Writes an entry to a shuffle dataset. Returns the size
   * in bytes of the data written.
   */
  public long put(ShuffleEntry entry) throws IOException;

  @Override
  public void close() throws IOException;
}
