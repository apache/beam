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

import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link PTransform} that provides a secure alternative to {@link
 * org.apache.beam.sdk.transforms.GroupByKey}.
 *
 * <p>This transform encrypts the keys of the input {@link PCollection}, performs a {@link
 * org.apache.beam.sdk.transforms.GroupByKey} on the encrypted keys, and then decrypts the keys in
 * the output. This is useful when the keys contain sensitive data that should not be stored at rest
 * by the runner.
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
    Coder<KV<K, V>> inputCoder = input.getCoder();
    if (!(inputCoder instanceof KvCoder)) {
      throw new IllegalStateException("GroupByEncryptedKey requires its input to use KvCoder");
    }
    KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) inputCoder;
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();
    Coder<V> valueCoder = inputKvCoder.getValueCoder();

    return input
        .apply("EncryptMessage", ParDo.of(new EncryptMessage<>(this.hmacKey, keyCoder, valueCoder)))
        .apply(GroupByKey.create())
        .apply(
            "DecryptMessage", ParDo.of(new DecryptMessage<>(this.hmacKey, keyCoder, valueCoder)));
  }

  private static class EncryptMessage<K, V> extends DoFn<KV<K, V>, KV<byte[], KV<byte[], byte[]>>> {
    private final Secret hmacKey;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Mac mac;
    private final Cipher cipher;

    EncryptMessage(Secret hmacKey, Coder<K> keyCoder, Coder<V> valueCoder) {
      this.hmacKey = hmacKey;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;

      try {
        this.mac = Mac.getInstance("HmacSHA256");
        this.mac.init(new SecretKeySpec(hmacKey.getSecretBytes(), "HmacSHA256"));
        this.cipher = Cipher.getInstance("AES");
        this.cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(hmacKey.getSecretBytes(), "AES"));
      } catch (Exception ex) {
        throw new RuntimeException(
            "Failed to initialize cryptography libraries needed for GroupByEncryptedKey", ex);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      byte[] encodedKey = encode(this.keyCoder, c.element().getKey());
      byte[] encodedValue = encode(this.valueCoder, c.element().getValue());

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

  private static class DecryptMessage<K, V>
      extends DoFn<KV<byte[], Iterable<KV<byte[], byte[]>>>, KV<K, Iterable<V>>> {
    private final Secret hmacKey;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private transient Cipher cipher;

    DecryptMessage(Secret hmacKey, Coder<K> keyCoder, Coder<V> valueCoder) {
      this.hmacKey = hmacKey;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;

      try {
        this.cipher = Cipher.getInstance("AES");
        this.cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(hmacKey.getSecretBytes(), "AES"));
      } catch (Exception ex) {
        throw new RuntimeException(
            "Failed to initialize cryptography libraries needed for GroupByEncryptedKey", ex);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      java.util.Map<K, java.util.List<V>> decryptedKvs = new java.util.HashMap<>();
      for (KV<byte[], byte[]> encryptedKv : c.element().getValue()) {
        byte[] decryptedKeyBytes = cipher.doFinal(encryptedKv.getKey());
        K key = decode(this.keyCoder, decryptedKeyBytes);

        if (key != null) {
          if (!decryptedKvs.containsKey(key)) {
            decryptedKvs.put(key, new java.util.ArrayList<>());
          }
          byte[] decryptedValueBytes = cipher.doFinal(encryptedKv.getValue());
          V value = decode(this.valueCoder, decryptedValueBytes);
          decryptedKvs.get(key).add(value);
        } else {
          throw new RuntimeException(
              "Found null key when decoding " + Arrays.toString(decryptedKeyBytes));
        }
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
