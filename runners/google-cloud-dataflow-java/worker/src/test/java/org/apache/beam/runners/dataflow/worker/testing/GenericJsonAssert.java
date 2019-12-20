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
import com.google.api.client.json.jackson2.JacksonFactory;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;

/** Assertions on {@link GenericJson} class. */
public class GenericJsonAssert {

  private static final JacksonFactory jacksonFactory = JacksonFactory.getDefaultInstance();

  /**
   * Asserts that {@code actual} has the same JSON representation as {@code expectedJsonText}.
   *
   * @param expectedJsonText expected JSON string.
   * @param actual actual object to compare its JSON representation.
   */
  public static void assertEqualsAsJson(String expectedJsonText, GenericJson actual) {
    actual.setFactory(jacksonFactory);

    String actualJsonText = actual.toString();
    try {
      JSONAssert.assertEquals(expectedJsonText, actualJsonText, true);
    } catch (JSONException ex) {
      throw new IllegalArgumentException("Could not parse JSON", ex);
    }
  }
}
