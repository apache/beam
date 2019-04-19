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

import org.apache.beam.sdk.values.KV;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class KvMatcher<K, V> extends TypeSafeMatcher<KV<? extends K, ? extends V>> {

  private final Matcher<? super K> keyMatcher;
  private final Matcher<? super V> valueMatcher;

  public KvMatcher(Matcher<? super K> keyMatcher,
      Matcher<? super V> valueMatcher) {
    this.keyMatcher = keyMatcher;
    this.valueMatcher = valueMatcher;
  }

  public static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher,
      Matcher<V> valueMatcher) {
    return new KvMatcher<>(keyMatcher, valueMatcher);
  }

  @Override
  public boolean matchesSafely(KV<? extends K, ? extends V> kv) {
    return keyMatcher.matches(kv.getKey())
        && valueMatcher.matches(kv.getValue());
  }

  @Override
  public void describeTo(Description description) {
    description
        .appendText("a KV(").appendValue(keyMatcher)
        .appendText(", ").appendValue(valueMatcher)
        .appendText(")");
  }

}
