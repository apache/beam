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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HttpBodyCoderTest {

  private static final HttpBodyCoder TEST_CODER = HttpBodyCoder.of();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (HttpBody value : FhirIOTestUtil.STU3_PRETTY_BUNDLES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }
}
