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
package org.apache.beam.runners.spark.util;

import java.util.Map;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Utility class for creating and managing side input readers in the Spark runner.
 *
 * <p>This class provides factory methods to create appropriate {@link SideInputReader}
 * implementations based on the execution mode (streaming or batch) to optimize side input access
 * patterns.
 */
public class SideInputReaderFactory {
  /**
   * Creates and returns a {@link SideInputReader} based on the configuration.
   *
   * <p>If streaming side inputs are enabled, returns a direct {@link SparkSideInputReader}.
   * Otherwise, returns a cached version of the side input reader using {@link
   * CachedSideInputReader} for better performance in batch processing.
   *
   * @param useStreamingSideInput Whether to use streaming side inputs
   * @param sideInputs A map of side inputs with their windowing strategies and broadcasts
   * @return A {@link SideInputReader} instance appropriate for the current configuration
   */
  public static SideInputReader create(
      boolean useStreamingSideInput,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs) {
    return useStreamingSideInput
        ? new SparkSideInputReader(sideInputs)
        : CachedSideInputReader.of(new SparkSideInputReader(sideInputs));
  }
}
