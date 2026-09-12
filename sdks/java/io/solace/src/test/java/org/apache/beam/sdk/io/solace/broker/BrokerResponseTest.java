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
package org.apache.beam.sdk.io.solace.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class BrokerResponseTest {

  /** An {@link java.io.InputStream} that records whether {@code close()} was invoked. */
  private static class CloseTrackingInputStream extends ByteArrayInputStream {
    private boolean closed = false;

    CloseTrackingInputStream(String data) {
      super(data.getBytes(StandardCharsets.UTF_8));
    }

    boolean wasClosed() {
      return closed;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  public void testContentIsReadAndStreamIsClosed() {
    CloseTrackingInputStream stream = new CloseTrackingInputStream("line1\nline2");

    BrokerResponse response = new BrokerResponse(200, "OK", stream);

    assertEquals("line1\nline2", response.content);
    assertTrue(
        "the content InputStream must be closed after the response body has been read",
        stream.wasClosed());
  }

  @Test
  public void testNullContentIsHandled() {
    BrokerResponse response = new BrokerResponse(204, "No Content", null);

    assertNull(response.content);
  }
}
