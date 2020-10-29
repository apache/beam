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
import java.lang.reflect.Method;
import java.util.Map;
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
  public Map<String, Method> userDefinedScalarFunctions() {
    try {
      return ImmutableMap.of(
          "fun",
          this.getClass().getMethod("matches", String.class, String.class),
          "foo",
          this.getClass().getMethod("foo"),
          "increment",
          this.getClass().getMethod("increment", Long.class),
          "isNull",
          this.getClass().getMethod("isNull", String.class));
    } catch (NoSuchMethodException e) {
      return ImmutableMap.of();
    }
  }

  @Override
  public Map<String, Combine.CombineFn<?, ?, ?>> userDefinedAggregateFunctions() {
    return ImmutableMap.of("agg_fun", Count.combineFn(), "custom_agg", new BitAnd<Long>());
  }

  public static boolean matches(String s, String regex) {
    return s.matches(regex);
  }

  public static String foo() {
    return "Hello world!";
  }

  public static Long increment(Long i) {
    return i + 1;
  }

  public static String notRegistered() {
    return "This method is not registered as a UDF.";
  }

  public static boolean isNull(String s) {
    return s == null;
  }

  /**
   * Bitwise-and is already a built-in function, but reimplement it here as a "user-defined"
   * function.
   */
  static class BitAnd<T extends Number> extends Combine.CombineFn<T, Long, Long> {
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
