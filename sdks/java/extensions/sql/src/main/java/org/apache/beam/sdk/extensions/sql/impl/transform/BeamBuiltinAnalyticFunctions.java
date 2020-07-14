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
package org.apache.beam.sdk.extensions.sql.impl.transform;

import java.util.Map;
import java.util.function.Function;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;

/** Built-in Analytic Functions for the aggregation analytics functionality. */
public class BeamBuiltinAnalyticFunctions {
  public static final Map<String, Function<Schema.FieldType, Combine.CombineFn<?, ?, ?>>>
      BUILTIN_ANALYTIC_FACTORIES =
          ImmutableMap.<String, Function<Schema.FieldType, Combine.CombineFn<?, ?, ?>>>builder()
              .putAll(BeamBuiltinAggregations.BUILTIN_AGGREGATOR_FACTORIES)
              // Pending Navigation functions
              // Pending Numbering functions
              .build();

  public static Combine.CombineFn<?, ?, ?> create(String functionName, Schema.FieldType fieldType) {
    Function<Schema.FieldType, Combine.CombineFn<?, ?, ?>> aggregatorFactory =
        BUILTIN_ANALYTIC_FACTORIES.get(functionName);
    if (aggregatorFactory != null) {
      return aggregatorFactory.apply(fieldType);
    }
    throw new UnsupportedOperationException(
        String.format("Analytics Function [%s] is not supported", functionName));
  }
}
