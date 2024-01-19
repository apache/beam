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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import com.google.auto.value.AutoValue;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Holds user defined function definitions. */
@AutoValue
public abstract class UserFunctionDefinitions {
  public abstract ImmutableMap<List<String>, ResolvedNodes.ResolvedCreateFunctionStmt>
      sqlScalarFunctions();

  /**
   * SQL native user-defined table-valued function can be resolved by Analyzer. Keeping the function
   * name to its ResolvedNode mapping so during Plan conversion, UDTVF implementation can replace
   * inputs of TVFScanConverter.
   */
  public abstract ImmutableMap<List<String>, ResolvedNode> sqlTableValuedFunctions();

  public abstract ImmutableMap<List<String>, JavaScalarFunction> javaScalarFunctions();

  public abstract ImmutableMap<List<String>, Combine.CombineFn<?, ?, ?>> javaAggregateFunctions();

  @AutoValue
  public abstract static class JavaScalarFunction {
    public static JavaScalarFunction create(Method method, String jarPath) {
      return new AutoValue_UserFunctionDefinitions_JavaScalarFunction(method, jarPath);
    }

    public abstract Method method();

    /** The Beam filesystem path to the jar where the method was defined. */
    public abstract String jarPath();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSqlScalarFunctions(
        ImmutableMap<List<String>, ResolvedNodes.ResolvedCreateFunctionStmt> sqlScalarFunctions);

    public abstract Builder setSqlTableValuedFunctions(
        ImmutableMap<List<String>, ResolvedNode> sqlTableValuedFunctions);

    public abstract Builder setJavaScalarFunctions(
        ImmutableMap<List<String>, JavaScalarFunction> javaScalarFunctions);

    public abstract Builder setJavaAggregateFunctions(
        ImmutableMap<List<String>, Combine.CombineFn<?, ?, ?>> javaAggregateFunctions);

    public abstract UserFunctionDefinitions build();
  }

  public static Builder newBuilder() {
    return new AutoValue_UserFunctionDefinitions.Builder()
        .setSqlScalarFunctions(ImmutableMap.of())
        .setSqlTableValuedFunctions(ImmutableMap.of())
        .setJavaScalarFunctions(ImmutableMap.of())
        .setJavaAggregateFunctions(ImmutableMap.of());
  }
}
