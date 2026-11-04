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
package org.apache.beam.sdk.io.couchbase;

import static org.junit.Assert.assertThrows;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonObjectCoderTest {

  private static final JsonObjectCoder CODER = JsonObjectCoder.of();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(CODER, JsonObject.create());
    CoderProperties.coderDecodeEncodeEqual(
        CODER,
        JsonObject.create()
            .put("name", "Air Ünïcode ✈")
            .put("id", 10)
            .put("passengers", 3_000_000_000L)
            .put("rating", 4.5)
            .put("active", true)
            .putNull("callsign")
            .put("routes", JsonArray.from("LHR", "JFK"))
            .put("hub", JsonObject.create().put("code", "LHR").put("terminals", 5)));
  }

  @Test
  public void testCoderSerializable() {
    CoderProperties.coderSerializable(CODER);
  }

  @Test
  public void testNotDeterministic() {
    assertThrows(NonDeterministicException.class, CODER::verifyDeterministic);
  }
}
