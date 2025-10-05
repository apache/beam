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
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.Secret;
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
 *
 * <p>The transform requires a {@link Secret} which returns a 32 byte secret which can be used to
 * generate a {@link SecretKeySpec} object using the HmacSHA256 algorithm.
 *
 * <p>Note the following caveats: 1) Runners can implement arbitrary materialization steps, so this
 * does not guarantee that the whole pipeline will not have unencrypted data at rest by itself. 2)
 * If using this transform in streaming mode, this transform may not properly handle update
 * compatibility checks around coders. This means that an improper update could lead to invalid
 * coders, causing pipeline failure or data corruption. If you need to update, make sure that the
 * input type passed into this transform does not change.
 */
public class GroupByEncryptedKey<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  private final Secret hmacKey;

  private GroupByEncryptedKey(Secret hmacKey) {
    this.hmacKey = hmacKey;
  }

  /**
   * Creates a {@link GroupByEncryptedKey} transform.
   *
   * @param hmacKey The {@link Secret} key to use for encryption.
   * @param <K> The type of the keys in the input PCollection.
   * @param <V> The type of the values in the input PCollection.
   * @return A {@link GroupByEncryptedKey} transform.
   */
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

    try {
      keyCoder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new IllegalStateException(
          "the keyCoder of a GroupByEncryptedKey must be deterministic", e);
    }

    Coder<V> valueCoder = inputKvCoder.getValueCoder();

    PCollection<KV<byte[], Iterable<KV<byte[], byte[]>>>> grouped =
        input
            .apply(
                "EncryptMessage",
                ParDo.of(new EncryptMessage<>(this.hmacKey, keyCoder, valueCoder)))
            .apply(GroupByKey.create());

    return grouped
        .apply("DecryptMessage", ParDo.of(new DecryptMessage<>(this.hmacKey, keyCoder, valueCoder)))
        .setCoder(KvCoder.of(keyCoder, IterableCoder.of(valueCoder)));
  }

  /**
   * A {@link PTransform} that encrypts the key and value of an element.
   *
   * <p>The resulting PCollection will be a KV pair with the key being the HMAC of the encoded key,
   * and the value being a KV pair of the encrypted key and value.
   */
  @SuppressWarnings("initialization.fields.uninitialized")
  private static class EncryptMessage<K, V> extends DoFn<KV<K, V>, KV<byte[], KV<byte[], byte[]>>> {
    private final Secret hmacKey;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private transient Mac mac;
    private transient Cipher cipher;
    private transient SecretKeySpec secretKeySpec;
    private transient java.security.SecureRandom generator;

    EncryptMessage(Secret hmacKey, Coder<K> keyCoder, Coder<V> valueCoder) {
      this.hmacKey = hmacKey;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Setup
    public void setup() {
      try {
        byte[] secretBytes = this.hmacKey.getSecretBytes();
        this.mac = Mac.getInstance("HmacSHA256");
        this.mac.init(new SecretKeySpec(secretBytes, "HmacSHA256"));
        this.cipher = Cipher.getInstance("AES/GCM/NoPadding");
        this.secretKeySpec = new SecretKeySpec(secretBytes, "AES");
      } catch (Exception ex) {
        throw new RuntimeException(
            "Failed to initialize cryptography libraries needed for GroupByEncryptedKey", ex);
      }
      this.generator = new java.security.SecureRandom();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      byte[] encodedKey = encode(this.keyCoder, c.element().getKey());
      byte[] encodedValue = encode(this.valueCoder, c.element().getValue());

      byte[] hmac = this.mac.doFinal(encodedKey);

      byte[] keyIv = new byte[12];
      byte[] valueIv = new byte[12];
      this.generator.nextBytes(keyIv);
      this.generator.nextBytes(valueIv);
      GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(128, keyIv);
      this.cipher.init(Cipher.ENCRYPT_MODE, this.secretKeySpec, gcmParameterSpec);
      byte[] encryptedKey = this.cipher.doFinal(encodedKey);
      gcmParameterSpec = new GCMParameterSpec(128, valueIv);
      this.cipher.init(Cipher.ENCRYPT_MODE, this.secretKeySpec, gcmParameterSpec);
      byte[] encryptedValue = this.cipher.doFinal(encodedValue);

      c.output(
          KV.of(
              hmac,
              KV.of(
                  com.google.common.primitives.Bytes.concat(keyIv, encryptedKey),
                  com.google.common.primitives.Bytes.concat(valueIv, encryptedValue))));
    }

    private <T> byte[] encode(Coder<T> coder, T value) throws Exception {
      java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
      coder.encode(value, os);
      return os.toByteArray();
    }
  }

  /**
   * A {@link PTransform} that decrypts the key and values of an element.
   *
   * <p>The input PCollection will be a KV pair with the key being the HMAC of the encoded key, and
   * the value being a list of KV pairs of the encrypted key and value.
   *
   * <p>This will return a tuple containing the decrypted key and a list of decrypted values.
   *
   * <p>Since there is some loss of precision in the HMAC encoding of the key (but not the key
   * encryption), there is some extra work done here to ensure that all key/value pairs are mapped
   * out appropriately.
   */
  @SuppressWarnings("initialization.fields.uninitialized")
  private static class DecryptMessage<K, V>
      extends DoFn<KV<byte[], Iterable<KV<byte[], byte[]>>>, KV<K, Iterable<V>>> {
    private final Secret hmacKey;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private transient Cipher cipher;
    private transient SecretKeySpec secretKeySpec;

    DecryptMessage(Secret hmacKey, Coder<K> keyCoder, Coder<V> valueCoder) {
      this.hmacKey = hmacKey;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Setup
    public void setup() {
      try {
        this.cipher = Cipher.getInstance("AES/GCM/NoPadding");
        this.secretKeySpec = new SecretKeySpec(this.hmacKey.getSecretBytes(), "AES");
      } catch (Exception ex) {
        throw new RuntimeException(
            "Failed to initialize cryptography libraries needed for GroupByEncryptedKey", ex);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      java.util.Map<K, java.util.List<V>> decryptedKvs = new java.util.HashMap<>();
      for (KV<byte[], byte[]> encryptedKv : c.element().getValue()) {
        byte[] iv = Arrays.copyOfRange(encryptedKv.getKey(), 0, 12);
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(128, iv);
        this.cipher.init(Cipher.DECRYPT_MODE, this.secretKeySpec, gcmParameterSpec);

        byte[] encryptedKey =
            Arrays.copyOfRange(encryptedKv.getKey(), 12, encryptedKv.getKey().length);
        byte[] decryptedKeyBytes = this.cipher.doFinal(encryptedKey);
        K key = decode(this.keyCoder, decryptedKeyBytes);

        if (key != null) {
          if (!decryptedKvs.containsKey(key)) {
            decryptedKvs.put(key, new java.util.ArrayList<>());
          }

          iv = Arrays.copyOfRange(encryptedKv.getValue(), 0, 12);
          gcmParameterSpec = new GCMParameterSpec(128, iv);
          this.cipher.init(Cipher.DECRYPT_MODE, this.secretKeySpec, gcmParameterSpec);

          byte[] encryptedValue =
              Arrays.copyOfRange(encryptedKv.getValue(), 12, encryptedKv.getValue().length);
          byte[] decryptedValueBytes = this.cipher.doFinal(encryptedValue);
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
