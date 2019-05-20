/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package test.util;

import static com.google.common.collect.Iterables.toArray;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static test.util.KvMatcher.isKv;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.Matcher;

public class ContainsKvs implements
    SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void> {

  private final List<KV<String, Iterable<String>>> expectedKvs;

  private ContainsKvs(List<KV<String, Iterable<String>>> expectedKvs) {
    this.expectedKvs = expectedKvs;
  }

  @SafeVarargs
  public static SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void> containsKvs(
      KV<String, Iterable<String>>... kvs) {
    return new ContainsKvs(ImmutableList.copyOf(kvs));
  }

  @Override
  public Void apply(Iterable<KV<String, Iterable<String>>> input) {
    List<Matcher<? super KV<String, Iterable<String>>>> matchers = new ArrayList<>();
    for (KV<String, Iterable<String>> expected : expectedKvs) {
      String[] values = toArray(expected.getValue(), String.class);
      matchers.add(isKv(equalTo(expected.getKey()), containsInAnyOrder(values)));
    }
    assertThat(input, containsInAnyOrder(matchers));
    return null;
  }

}
