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
package org.apache.beam.sdk.extensions.sql.zetasql.provider;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.AggregateFn;
import org.apache.beam.sdk.extensions.sql.ApplyMethod;
import org.apache.beam.sdk.extensions.sql.ScalarFn;
import org.apache.beam.sdk.extensions.sql.UdfProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Defines Java UDFs that are tested in {@link
 * org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlJavaUdfTest}.
 */
@AutoService(UdfProvider.class)
public class UdfTestProvider implements UdfProvider {
  @Override
  public Map<String, ScalarFn> userDefinedScalarFunctions() {
    return ImmutableMap.of(
        "matches",
        new MatchFn(),
        "helloWorld",
        new HelloWorldFn(),
        "increment",
        new IncrementFn(),
        "isNull",
        new IsNullFn());
  }

  public static class MatchFn extends ScalarFn {
    @ApplyMethod
    public boolean matches(String s, String regex) {
      return s.matches(regex);
    }
  }

  public static class HelloWorldFn extends ScalarFn {
    @ApplyMethod
    public String helloWorld() {
      return "Hello world!";
    }
  }

  public static class IncrementFn extends ScalarFn {
    @ApplyMethod
    public Long increment(Long i) {
      return i + 1;
    }
  }

  public static class IsNullFn extends ScalarFn {
    @ApplyMethod
    public boolean isNull(String s) {
      return s == null;
    }
  }

  public static class UnusedFn extends ScalarFn {
    @ApplyMethod
    public String notRegistered() {
      return "This method is not registered as a UDF.";
    }
  }

  @Override
  public Map<String, AggregateFn> userDefinedAggregateFunctions() {
    return ImmutableMap.of(
        "agg_fun",
        AggregateFn.fromCombineFn(Count.combineFn()),
        "custom_agg",
        AggregateFn.fromCombineFn(new BitAnd<Long>()));
  }

  /**
   * Bitwise-and is already a built-in function, but reimplement it here as a "user-defined"
   * function.
   */
  public static class BitAnd<T extends Number> extends Combine.CombineFn<T, Long, Long> {
    @Override
    public Long createAccumulator() {
      return -1L;
    }

    @Override
    public Long addInput(Long accum, T input) {
      return accum & input.longValue();
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accums) {
      Long merged = createAccumulator();
      for (Long accum : accums) {
        merged = merged & accum;
      }
      return merged;
    }

    @Override
    public Long extractOutput(Long accum) {
      return accum;
    }
  }
}
