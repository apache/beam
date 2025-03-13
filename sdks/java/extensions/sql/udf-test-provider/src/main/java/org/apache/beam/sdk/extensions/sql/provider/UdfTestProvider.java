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
package org.apache.beam.sdk.extensions.sql.provider;

import com.google.auto.service.AutoService;
import java.sql.Date;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.udf.AggregateFn;
import org.apache.beam.sdk.extensions.sql.udf.ScalarFn;
import org.apache.beam.sdk.extensions.sql.udf.UdfProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Defines Java UDFs for use in tests. */
@AutoService(UdfProvider.class)
public class UdfTestProvider implements UdfProvider {
  @Override
  public Map<String, ScalarFn> userDefinedScalarFunctions() {
    return ImmutableMap.of(
        "helloWorld",
        new HelloWorldFn(),
        "matches",
        new MatchFn(),
        "increment",
        new IncrementFn(),
        "isNull",
        new IsNullFn(),
        "dateIncrementAll",
        new DateIncrementAllFn());
  }

  @Override
  public Map<String, AggregateFn<?, ?, ?>> userDefinedAggregateFunctions() {
    return ImmutableMap.of("my_sum", new Sum());
  }

  public static class HelloWorldFn extends ScalarFn {
    @ApplyMethod
    public String helloWorld() {
      return "Hello world!";
    }
  }

  public static class MatchFn extends ScalarFn {
    @ApplyMethod
    public boolean matches(String s, String regex) {
      return s.matches(regex);
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

  public static class Sum implements AggregateFn<Long, Long, Long> {

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long mutableAccumulator, Long input) {
      return mutableAccumulator + input;
    }

    @Override
    public Long mergeAccumulators(Long mutableAccumulator, Iterable<Long> immutableAccumulators) {
      for (Long x : immutableAccumulators) {
        mutableAccumulator += x;
      }
      return mutableAccumulator;
    }

    @Override
    public Long extractOutput(Long mutableAccumulator) {
      return mutableAccumulator;
    }
  }

  public static class DateIncrementAllFn extends ScalarFn {
    @ApplyMethod
    public Date incrementAll(Date date) {
      return new Date(date.getYear() + 1, date.getMonth() + 1, date.getDate() + 1);
    }
  }
}
