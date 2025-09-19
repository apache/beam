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
package org.apache.beam.sdk.transforms;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link PTransform} that provides a secure alternative to {@link GroupByKey}.
 *
 * <p>This transform encrypts the keys of the input {@link PCollection}, performs a {@link
 * GroupByKey} on the encrypted keys, and then decrypts the keys in the output. This is useful when
 * the keys contain sensitive data that should not be stored at rest by the runner.
 */
public class GroupByEncryptedKey<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  private final Secret hmacKey;

  private GroupByEncryptedKey(Secret hmacKey) {
    this.hmacKey = hmacKey;
  }

  public static <K, V> GroupByEncryptedKey<K, V> create(Secret hmacKey) {
    return new GroupByEncryptedKey<>(hmacKey);
  }

  @Override
  public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
    return input
        .apply("EncryptMessage", ParDo.of(new _EncryptMessage<>()))
        .apply(GroupByKey.create())
        .apply("DecryptMessage", ParDo.of(new _DecryptMessage<>()));
  }

  private static class _EncryptMessage<K, V> extends DoFn<KV<K, V>, KV<byte[], KV<byte[], byte[]>>> {
    private final Secret hmacKey;
    private transient Mac mac;
    private transient Cipher cipher;

    _EncryptMessage(Secret hmacKey) {
      this.hmacKey = hmacKey;
    }

    @Setup
    public void setup() throws Exception {
      mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(hmacKey.getSecretBytes(), "HmacSHA256"));
      cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(hmacKey.getSecretBytes(), "AES"));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Coder<K> keyCoder = ((KvCoder<K, V>) c.getPipeline().getCoderRegistry().getCoder(c.element().getClass())).getKeyCoder();
      Coder<V> valueCoder = ((KvCoder<K, V>) c.getPipeline().getCoderRegistry().getCoder(c.element().getClass())).getValueCoder();

      byte[] encodedKey = encode(keyCoder, c.element().getKey());
      byte[] encodedValue = encode(valueCoder, c.element().getValue());

      byte[] hmac = mac.doFinal(encodedKey);
      byte[] encryptedKey = cipher.doFinal(encodedKey);
      byte[] encryptedValue = cipher.doFinal(encodedValue);

      c.output(KV.of(hmac, KV.of(encryptedKey, encryptedValue)));
    }

    private <T> byte[] encode(Coder<T> coder, T value) throws Exception {
      java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
      coder.encode(value, os);
      return os.toByteArray();
    }
  }

  private static class _DecryptMessage<K, V>
      extends DoFn<KV<byte[], Iterable<KV<byte[], byte[]>>>, KV<K, Iterable<V>>> {
    private final Secret hmacKey;
    private transient Cipher cipher;

    _DecryptMessage(Secret hmacKey) {
      this.hmacKey = hmacKey;
    }

    @Setup
    public void setup() throws Exception {
      cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(hmacKey.getSecretBytes(), "AES"));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Coder<K> keyCoder = ((KvCoder<K, V>) c.getPipeline().getCoderRegistry().getCoder(c.element().getClass())).getKeyCoder();
      Coder<V> valueCoder = ((KvCoder<K, V>) c.getPipeline().getCoderRegistry().getCoder(c.element().getClass())).getValueCoder();

      java.util.Map<K, java.util.List<V>> decryptedKvs = new java.util.HashMap<>();
      for (KV<byte[], byte[]> encryptedKv : c.element().getValue()) {
        byte[] decryptedKeyBytes = cipher.doFinal(encryptedKv.getKey());
        K key = decode(keyCoder, decryptedKeyBytes);

        if (!decryptedKvs.containsKey(key)) {
          decryptedKvs.put(key, new java.util.ArrayList<>());
        }
        byte[] decryptedValueBytes = cipher.doFinal(encryptedKv.getValue());
        V value = decode(valueCoder, decryptedValueBytes);
        decryptedKvs.get(key).add(value);
      }

      for (java.util.Map.Entry<K, java.util.List<V>> entry : decryptedKvs.entrySet()) {
        c.output(KV.of(entry.getKey(), entry.getValue()));
      }
    }

    private <T> T decode(Coder<T> coder, byte[] bytes) throws Exception {
      java.io.ByteArrayInputStream is = new java.io.ByteArrayInputStream(bytes);
      return coder.decode(is);
    }
  }
}
