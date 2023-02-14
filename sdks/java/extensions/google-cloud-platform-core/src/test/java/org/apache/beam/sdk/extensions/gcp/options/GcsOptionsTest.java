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
package org.apache.beam.sdk.extensions.gcp.options;

import static org.junit.Assert.assertEquals;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsOptionsTest {

  @Test
  public void testGetSetHttpTransport() throws IOException {
    GcsOptions options = PipelineOptionsFactory.as(GcsOptions.class);
    options.setGcsHttpTransport(new MockHttpTransport());
    GcsUtil.CreateOptions createOptions = GcsUtil.CreateOptions.builder().build();
    GcsPath fakePath = GcsPath.fromUri("gs://some-path/somefile");
    try (WritableByteChannel channel = options.getGcsUtil().create(fakePath, createOptions)) {
      channel.write(ByteBuffer.wrap("some bytes".getBytes(Charset.defaultCharset())));
    } catch (NullPointerException e) {
      // this is expected because we returned null in buildRequest
    }
    MockHttpTransport usedTransport = (MockHttpTransport) options.getGcsHttpTransport();
    // assert that  the custom http transport has been invoked
    assertEquals(1, usedTransport.getRequestCount());
  }

  static class MockHttpTransport extends HttpTransport {
    private int requestCount;

    @Override
    protected LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
      requestCount += 1;
      return null;
    }

    public int getRequestCount() {
      return requestCount;
    }
  }
}
