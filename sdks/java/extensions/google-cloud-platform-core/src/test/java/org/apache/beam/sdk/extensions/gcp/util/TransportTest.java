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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link Transport}. */
@RunWith(JUnit4.class)
public class TransportTest {

  private static class MockHttpTransport extends HttpTransport {

    @Override
    protected LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
      return null;
    }
  }

  @Test
  public void testGetSetTransport() {
    HttpTransport defaultTransport = Transport.getTransport();
    try {
      Transport.setHttpTransport(new MockHttpTransport());
      assertEquals(Transport.getTransport().getClass(), MockHttpTransport.class);
    } finally {
      // reset to default transport instance
      Transport.setHttpTransport(null);
      assertTrue(Transport.getTransport() == defaultTransport);
    }
  }
}
