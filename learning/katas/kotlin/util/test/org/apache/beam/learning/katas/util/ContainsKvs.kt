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
package org.apache.beam.learning.katas.util

import com.google.common.collect.ImmutableList
import com.google.common.collect.Iterables
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.hamcrest.CoreMatchers
import org.hamcrest.Matcher
import org.hamcrest.collection.IsIterableContainingInAnyOrder
import org.junit.Assert
import java.util.*

class ContainsKvs private constructor(private val expectedKvs: List<KV<String, Iterable<String>>>) :
  SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void?> {

  companion object {
    @SafeVarargs
    fun containsKvs(vararg kvs: KV<String, Iterable<String>>): SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void?> {
      return ContainsKvs(ImmutableList.copyOf(kvs))
    }
  }

  override fun apply(input: Iterable<KV<String, Iterable<String>>>): Void? {
    val matchers: MutableList<Matcher<in KV<String, Iterable<String>>>> = ArrayList()

    for (expected in expectedKvs) {
      val values = Iterables.toArray(expected.value, String::class.java)
      matchers.add(
        KvMatcher.isKv(
          CoreMatchers.equalTo(expected.key),
          IsIterableContainingInAnyOrder.containsInAnyOrder(*values)
        )
      )
    }

    Assert.assertThat(input, IsIterableContainingInAnyOrder.containsInAnyOrder(matchers))

    return null
  }

}