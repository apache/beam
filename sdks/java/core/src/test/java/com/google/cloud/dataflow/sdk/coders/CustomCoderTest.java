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

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.values.KV;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Unit tests for {@link CustomCoder}. */
@RunWith(JUnit4.class)
public class CustomCoderTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(new MyCustomCoder("foo"),
        MyCustomCoder.class.getCanonicalName());
  }

  @Test
  public void testAnonymousEncodingIdError() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Anonymous CustomCoder subclass");
    thrown.expectMessage("must override getEncodingId()");
    new CustomCoder<Integer>() {

      @Override
      public void encode(Integer kv, OutputStream out, Context context) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Integer decode(InputStream inStream, Context context) {
        throw new UnsupportedOperationException();
      }
    }.getEncodingId();
  }

  @Test
  public void testAnonymousEncodingIdOk() throws Exception {
    new CustomCoder<Integer>() {

      @Override
      public void encode(Integer kv, OutputStream out, Context context) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Integer decode(InputStream inStream, Context context) {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getEncodingId() {
        return "A user must specify this. It can contain any character, including these: !$#%$@.";
      }
    }.getEncodingId();
  }
}
