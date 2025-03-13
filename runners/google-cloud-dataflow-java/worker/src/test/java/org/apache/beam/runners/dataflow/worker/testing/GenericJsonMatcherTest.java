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

import static org.apache.beam.runners.dataflow.worker.testing.GenericJsonMatcher.jsonOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.api.client.json.GenericJson;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GenericJsonMatcher}. */
@RunWith(JUnit4.class)
public class GenericJsonMatcherTest {

  @Test
  public void testMatch() {
    GenericJson expected = new GenericJson();
    expected.set("foo", "bar");
    GenericJson actual = new GenericJson();
    actual.set("foo", "bar");

    assertThat(expected, is(jsonOf(actual)));
  }

  @Test
  public void testMatchFailure() {
    GenericJson expected = new GenericJson();
    expected.set("foo", "expected");
    GenericJson actual = new GenericJson();
    actual.set("foo", "actual");

    try {
      assertThat(actual, is(jsonOf(expected)));
    } catch (AssertionError ex) {
      assertEquals(
          "\nExpected: is {\"foo\":\"expected\"}\n"
              + "     but: was <GenericData{classInfo=[], {foo=actual}}>",
          ex.getMessage());

      // pass
      return;
    }
    fail("The difference in JSON should raise AssertionError");
  }
}
