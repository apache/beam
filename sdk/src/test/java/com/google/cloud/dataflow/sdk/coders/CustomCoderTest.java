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

import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.values.KV;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Unit tests for {@link CustomCoder}. */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class CustomCoderTest {

  private static class MyCustomCoder extends CustomCoder<KV<String, Long>> {
    private final String key;

    public MyCustomCoder(String key) {
      this.key = key;
    }

    @Override
    public void encode(KV<String, Long> kv, OutputStream out, Context context)
            throws IOException {
      new DataOutputStream(out).writeLong(kv.getValue());
    }

    @Override
    public KV<String, Long> decode(InputStream inStream, Context context)
        throws IOException {
      return KV.of(key, new DataInputStream(inStream).readLong());
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof MyCustomCoder
          && key.equals(((MyCustomCoder) other).key);
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
    Assert.assertEquals(
        KV.of("key", 3L), CoderUtils.decodeFromByteArray(coder, encoded2));
  }

  @Test
  public void testEncodable() throws Exception {
    SerializableUtils.ensureSerializable(new MyCustomCoder("key"));
  }
}
