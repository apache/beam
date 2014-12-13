/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link URICoder}. */
@RunWith(JUnit4.class)
public class URICoderTest {

  // Test data

  private static final List<String> TEST_URI_STRINGS = Arrays.asList(
      "http://www.example.com",
      "gs://myproject/mybucket/a/gcs/path",
      "/just/a/path",
      "file:/path/with/no/authority",
      "file:///path/with/empty/authority");

  private static final List<Coder.Context> TEST_CONTEXTS = Arrays.asList(
      Coder.Context.OUTER,
      Coder.Context.NESTED);

  // Tests

  @Test
  public void testDeterministic() throws Exception {
    Coder<URI> coder = URICoder.of();

    for (String uriString : TEST_URI_STRINGS) {
      for (Coder.Context context : TEST_CONTEXTS) {
        // Obviously equal, but distinct as objects
        CoderProperties.coderDeterministic(coder, context, new URI(uriString), new URI(uriString));
      }
    }
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    Coder<URI> coder = URICoder.of();

    for (String uriString : TEST_URI_STRINGS) {
      for (Coder.Context context : TEST_CONTEXTS) {
        CoderProperties.coderDecodeEncodeEqual(coder, context, new URI(uriString));
      }
    }
  }
}
