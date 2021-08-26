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
package org.apache.beam.sdk.extensions.sql.udf;

import java.util.Collections;
import java.util.Map;

/**
 * Provider for user-defined functions written in Java. Implementations should be annotated with
 * {@link com.google.auto.service.AutoService}.
 */
public interface UdfProvider {
  /** Maps function names to scalar function implementations. */
  default Map<String, ScalarFn> userDefinedScalarFunctions() {
    return Collections.emptyMap();
  }

  /** Maps function names to aggregate function implementations. */
  default Map<String, AggregateFn<?, ?, ?>> userDefinedAggregateFunctions() {
    return Collections.emptyMap();
  }
}
