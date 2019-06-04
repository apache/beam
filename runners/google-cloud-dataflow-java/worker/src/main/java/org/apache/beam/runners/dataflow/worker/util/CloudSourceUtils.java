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
package org.apache.beam.runners.dataflow.worker.util;

import com.google.api.services.dataflow.model.Source;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;

/** Utilities for working with Source Dataflow API definitions and {@link NativeReader} objects. */
public class CloudSourceUtils {
  /**
   * Returns a copy of the source with {@code baseSpecs} flattened into {@code spec}. On conflict
   * for a parameter name, values in {@code spec} override values in {@code baseSpecs}, and later
   * values in {@code baseSpecs} override earlier ones.
   */
  public static Source flattenBaseSpecs(Source source) {
    if (source.getBaseSpecs() == null) {
      return source;
    }
    Map<String, Object> params = new HashMap<>();
    for (Map<String, Object> baseSpec : source.getBaseSpecs()) {
      params.putAll(baseSpec);
    }
    params.putAll(source.getSpec());

    Source result = source.clone();
    result.setSpec(params);
    result.setBaseSpecs(null);
    return result;
  }
}
