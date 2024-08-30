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
package org.apache.beam.sdk.testing;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher to compare a string or byte[] representing a JSON Object, independent of field order.
 *
 * <pre>
 *   assertThat("{\"name\": \"person\", \"height\": 80}",
 *              jsonStringLike("{\"height\": 80, \"name\": \"person\"}"));
 * </pre>
 */
public abstract class JsonMatcher<T> extends TypeSafeMatcher<T> {
  private final Matcher<Map<String, Object>> mapMatcher;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private @MonotonicNonNull Map<String, Object> actualMap;

  public JsonMatcher(Map<String, Object> expectedMap) {
    this.mapMatcher = is(expectedMap);
  }

  protected abstract Map<String, Object> parse(T json) throws IOException;

  public static Matcher<byte[]> jsonBytesLike(String json) throws IOException {
    Map<String, Object> fields =
        MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
    return jsonBytesLike(fields);
  }

  public static Matcher<byte[]> jsonBytesLike(Map<String, Object> fields) throws IOException {
    return new JsonMatcher<byte[]>(fields) {
      @Override
      protected Map<String, Object> parse(byte[] json) throws IOException {
        return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
      }
    };
  }

  public static Matcher<String> jsonStringLike(String json) throws IOException {
    Map<String, Object> fields =
        MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
    return jsonStringLike(fields);
  }

  public static Matcher<String> jsonStringLike(Map<String, Object> fields) throws IOException {
    return new JsonMatcher<String>(fields) {
      @Override
      protected Map<String, Object> parse(String json) throws IOException {
        return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
      }
    };
  }

  @Override
  protected boolean matchesSafely(T actual) {
    try {
      actualMap = parse(actual);
    } catch (IOException e) {
      return false;
    }
    return mapMatcher.matches(checkStateNotNull(actualMap));
  }

  @Override
  public void describeTo(Description description) {
    mapMatcher.describeTo(description);
  }

  @Override
  protected void describeMismatchSafely(T item, Description mismatchDescription) {
    mapMatcher.describeMismatch(checkStateNotNull(actualMap), mismatchDescription);
  }
}
