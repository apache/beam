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
package org.apache.beam.fn.harness.debug;

import javax.annotation.Nullable;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A record class that wraps an element sample with additional metadata. This ensures the ability to
 * add an exception to a sample even if it is pushed out of the buffer.
 *
 * @param <T> the element type of the PCollection.
 */
public class ElementSample<T> {
  // Used for deduplication purposes. Randomly chosen, should be unique within a given
  // OutputSampler.
  public final long id;

  // The element sample to be serialized and later queried.
  public final WindowedValue<T> sample;

  public static class ExceptionMetadata {
    ExceptionMetadata(String message, String ptransformId) {
      this.message = message;
      this.ptransformId = ptransformId;
    }

    // The stringified exception that caused the bundle to fail.
    public final String message;

    // The PTransform of where the exception occurred first.
    public final String ptransformId;
  }

  // An optional exception to be given as metadata on the FnApi for the given sample.
  @Nullable public ExceptionMetadata exception = null;

  ElementSample(long id, WindowedValue<T> sample) {
    this.id = id;
    this.sample = sample;
  }
}
