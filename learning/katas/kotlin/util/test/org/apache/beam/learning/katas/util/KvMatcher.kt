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
package org.apache.beam.learning.katas.util

import org.apache.beam.sdk.values.KV
import org.hamcrest.Description
import org.hamcrest.Matcher
import org.hamcrest.TypeSafeMatcher

class KvMatcher<K, V>(
  private val keyMatcher: Matcher<in K>,
  private val valueMatcher: Matcher<in V>
) : TypeSafeMatcher<KV<out K, out V>>() {

  companion object {
    fun <K, V> isKv(keyMatcher: Matcher<K>, valueMatcher: Matcher<V>): KvMatcher<K, V> {
      return KvMatcher(keyMatcher, valueMatcher)
    }
  }

  public override fun matchesSafely(kv: KV<out K, out V>): Boolean {
    return (keyMatcher.matches(kv.key) && valueMatcher.matches(kv.value))
  }

  override fun describeTo(description: Description) {
    description
      .appendText("a KV(").appendValue(keyMatcher)
      .appendText(", ").appendValue(valueMatcher)
      .appendText(")")
  }

}