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
package org.apache.beam.sdk.coders;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CustomCoder}. */
@RunWith(JUnit4.class)
public class CustomCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static class MyCustomCoder extends CustomCoder<KV<String, Long>> {
    private final String key;

    public MyCustomCoder(String key) {
      this.key = key;
    }

    @Override
    public void encode(KV<String, Long> kv, OutputStream out) throws IOException {
      new DataOutputStream(out).writeLong(kv.getValue());
    }

    @Override
    public KV<String, Long> decode(InputStream inStream) throws IOException {
      return KV.of(key, new DataInputStream(inStream).readLong());
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof MyCustomCoder && key.equals(((MyCustomCoder) other).key);
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }
  }

  @Test
  public void testEncodeDecode() throws Exception {
    MyCustomCoder coder = new MyCustomCoder("key");
    CoderProperties.coderDecodeEncodeEqual(coder, KV.of("key", 3L));

    byte[] encoded2 = CoderUtils.encodeToByteArray(coder, KV.of("ignored", 3L));
    Assert.assertEquals(KV.of("key", 3L), CoderUtils.decodeFromByteArray(coder, encoded2));
  }

  @Test
  public void testEncodable() throws Exception {
    SerializableUtils.ensureSerializable(new MyCustomCoder("key"));
  }
}
