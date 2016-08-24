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
package org.apache.beam.sdk.microbenchmarks.coders;

import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;

/**
 * Utilities for writing coder benchmarks.
 */
class CoderBenchmarking {

  /**
   * Encodes and decodes the given value using the specified Coder.
   *
   * @throws IOException if there are errors during encoding or decoding
   */
  public static <T> T testCoder(
      Coder<T> coder, boolean isWholeStream, T value) throws IOException {
    Coder.Context context =
        isWholeStream ? Coder.Context.OUTER : Coder.Context.NESTED;
    byte[] encoded = CoderUtils.encodeToByteArray(coder, value, context);
    return CoderUtils.decodeFromByteArray(coder, encoded, context);
  }
}
