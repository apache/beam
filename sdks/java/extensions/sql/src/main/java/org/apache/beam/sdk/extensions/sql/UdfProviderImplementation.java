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
package org.apache.beam.sdk.extensions.sql;

import com.google.auto.service.AutoService;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

@AutoService(UdfProviderImplementation.class)
public class UdfProviderImplementation implements UdfProvider {

  @Override
  public Map<String, Method> getUdfs() {
    try {
      return ImmutableMap.of(
          "fun", this.getClass().getMethod("matches", String.class, String.class));
    } catch (NoSuchMethodException e) {
      return ImmutableMap.of();
    }
  }

  @Override
  public Map<String, CombineFn> getUdafs() {
    return ImmutableMap.of("agg_fun", Count.combineFn());
  }

  @Override
  public Map<String, PTransform<PCollection<Row>, PCollection<Row>>> getUdtvfs() {
    throw new UnsupportedOperationException();
  }

  public static boolean matches(String s, String regex) {
    return s.matches(regex);
  }
}
