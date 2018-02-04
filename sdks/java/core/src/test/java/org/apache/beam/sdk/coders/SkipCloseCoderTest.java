/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.coders;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Ensures a coder can be decorated with the skipclose one
 * to support codecs needing to call close() (for memory releasing reason for instance)
 * and having as side effect to call close() on the underlying stream.
 */
public class SkipCloseCoderTest {
  @Test
  public void roundTrip() throws IOException {
    final SkipCloseCoder<String> coder = new SkipCloseCoder<>(new BuggyCoder());
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    coder.encode("ok", outStream);
    assertEquals("ok", coder.decode(new ByteArrayInputStream(outStream.toByteArray())));
  }

  @Test
  public void outputCloseIgnored() throws IOException {
    final SkipCloseCoder<String> coder = new SkipCloseCoder<>(new BuggyCoder());
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    coder.encode("ok", outStream);
    coder.encode("yes", outStream);
    assertEquals("okyes", coder.decode(new ByteArrayInputStream(outStream.toByteArray())));
  }

  @Test
  public void inputCloseIgnored() throws IOException {
    final SkipCloseCoder<String> coder = new SkipCloseCoder<>(new BuggyCoder());
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    coder.encode("ok", outStream);
    assertEquals("ok", coder.decode(new ByteArrayInputStream(outStream.toByteArray())));
    assertEquals("ok", coder.decode(new ByteArrayInputStream(outStream.toByteArray())));
  }

  /**
   * A coder calling close on streams.
   */
  public static class BuggyCoder extends CustomCoder<String> {

    @Override
    public void encode(final String value, final OutputStream outStream) throws IOException {
      outStream.write(value.getBytes(StandardCharsets.UTF_8));
      outStream.close();
    }

    @Override
    public String decode(final InputStream inStream) throws IOException {
      final byte[] buffer = new byte[1024];
      final int read = inStream.read(buffer);
      inStream.close();
      return new String(buffer, 0, read);
    }
  }
}
