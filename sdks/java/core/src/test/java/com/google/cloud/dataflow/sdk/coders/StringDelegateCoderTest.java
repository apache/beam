/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.Coder.NonDeterministicException;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link StringDelegateCoder}. */
@RunWith(JUnit4.class)
public class StringDelegateCoderTest {

  // Test data

  private static final Coder<URI> uriCoder = StringDelegateCoder.of(URI.class);

  private static final List<String> TEST_URI_STRINGS = Arrays.asList(
      "http://www.example.com",
      "gs://myproject/mybucket/some/gcs/path",
      "/just/some/path",
      "file:/path/with/no/authority",
      "file:///path/with/empty/authority");

  // Tests

  @Test
  public void testDeterministic() throws Exception, NonDeterministicException {
    uriCoder.verifyDeterministic();
    for (String uriString : TEST_URI_STRINGS) {
      CoderProperties.coderDeterministic(uriCoder, new URI(uriString), new URI(uriString));
    }
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (String uriString : TEST_URI_STRINGS) {
      CoderProperties.coderDecodeEncodeEqual(uriCoder, new URI(uriString));
    }
  }

  @Test
  public void testSerializable() throws Exception {
    CoderProperties.coderSerializable(uriCoder);
  }

  @Test
  public void testEncodingId() throws Exception {
    StringDelegateCoder<URI> coder = StringDelegateCoder.of(URI.class);
    CoderProperties.coderHasEncodingId(coder, URI.class.getName());
  }
}
