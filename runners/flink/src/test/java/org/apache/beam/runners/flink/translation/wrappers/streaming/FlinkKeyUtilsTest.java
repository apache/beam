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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Test;

/** Tests for {@link FlinkKeyUtils}. */
public class FlinkKeyUtilsTest {

  @Test
  public void testEncodeDecode() {
    String key = "key";
    StringUtf8Coder coder = StringUtf8Coder.of();

    ByteBuffer byteBuffer = FlinkKeyUtils.encodeKey(key, coder);
    assertThat(FlinkKeyUtils.decodeKey(byteBuffer, coder), is(key));
  }

  @Test
  public void testNullKey() {
    Void key = null;
    VoidCoder coder = VoidCoder.of();

    ByteBuffer byteBuffer = FlinkKeyUtils.encodeKey(key, coder);
    assertThat(FlinkKeyUtils.decodeKey(byteBuffer, coder), is(nullValue()));
  }

  @Test
  @SuppressWarnings("ByteBufferBackingArray")
  public void testCoderContext() throws Exception {
    String input = "hello world";
    Coder<String> coder = StringUtf8Coder.of();

    ByteBuffer encoded = FlinkKeyUtils.encodeKey(input, coder);
    // Ensure NESTED context is used
    assertThat(
        encoded.array(), is(CoderUtils.encodeToByteArray(coder, input, Coder.Context.NESTED)));
  }

  @Test
  @SuppressWarnings("ByteBufferBackingArray")
  public void testFromEncodedKey() {
    ByteString input = ByteString.copyFrom("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuffer encodedKey = FlinkKeyUtils.fromEncodedKey(input);
    assertThat(encodedKey.array(), is(input.toByteArray()));
  }
}
