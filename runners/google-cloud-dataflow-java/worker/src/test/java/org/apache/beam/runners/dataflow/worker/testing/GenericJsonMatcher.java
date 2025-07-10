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
package org.apache.beam.runners.dataflow.worker.testing;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.gson.GsonFactory;
import java.io.IOException;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;

/**
 * Matcher to compare {@link GenericJson}s using JSONassert's {@link JSONCompare}. This matcher does
 * not rely on {@link GenericJson#equals(Object)}, which may use fields irrelevant to JSON values.
 */
public final class GenericJsonMatcher extends TypeSafeMatcher<GenericJson> {

  private String expectedJsonText;

  private static final GsonFactory gsonFactory = GsonFactory.getDefaultInstance();

  private GenericJsonMatcher(GenericJson expected) {
    try {
      expectedJsonText = gsonFactory.toString(expected);
    } catch (IOException ex) {
      throw new IllegalArgumentException("Could not parse JSON", ex);
    }
  }

  public static GenericJsonMatcher jsonOf(GenericJson genericJson) {
    return new GenericJsonMatcher(genericJson);
  }

  @Override
  protected boolean matchesSafely(GenericJson actual) {
    try {
      String actualJsonText = gsonFactory.toString(actual);
      JSONCompareResult result =
          JSONCompare.compareJSON(expectedJsonText, actualJsonText, JSONCompareMode.STRICT);
      return result.passed();
    } catch (IOException | JSONException ex) {
      throw new IllegalArgumentException("Could not parse JSON", ex);
    }
  }

  @Override
  public void describeTo(Description description) {
    description.appendText(expectedJsonText);
  }
}
