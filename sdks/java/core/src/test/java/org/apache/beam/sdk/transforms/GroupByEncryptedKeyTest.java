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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.RawSecret;
import org.apache.beam.sdk.util.Secret;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GroupByEncryptedKey}. */
@RunWith(JUnit4.class)
public class GroupByEncryptedKeyTest implements Serializable {

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static class FakeSecret extends Secret {
    private final byte[] secret =
        "YUt3STJQbXFZRnQycDV0TktDeUJTNXFZV0hoSHNHWmM".getBytes(Charset.defaultCharset());

    @Override
    public byte[] getSecretBytes() {
      return secret;
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByKeyFakeSecret() {
    List<KV<String, Integer>> ungroupedPairs =
        Arrays.asList(
            KV.of("k1", 3),
            KV.of("k5", Integer.MAX_VALUE),
            KV.of("k5", Integer.MIN_VALUE),
            KV.of("k2", 66),
            KV.of("k1", 4),
            KV.of("k2", -33),
            KV.of("k3", 0));

    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(ungroupedPairs)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByEncryptedKey.<String, Integer>create(new FakeSecret()));

    PAssert.that(output.apply("Sort", MapElements.via(new SortValues())))
        .containsInAnyOrder(
            KV.of("k1", Arrays.asList(3, 4)),
            KV.of("k5", Arrays.asList(Integer.MIN_VALUE, Integer.MAX_VALUE)),
            KV.of("k2", Arrays.asList(-33, 66)),
            KV.of("k3", Arrays.asList(0)));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByKeyRawSecret() {
    List<KV<@Nullable String, Integer>> ungroupedPairs =
        Arrays.asList(
            KV.of(null, 3),
            KV.of("k1", 3),
            KV.of("k5", Integer.MAX_VALUE),
            KV.of("k5", Integer.MIN_VALUE),
            KV.of("k2", 66),
            KV.of("k1", 4),
            KV.of(null, 5),
            KV.of("k2", -33),
            KV.of("k3", 0));

    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(ungroupedPairs)
                .withCoder(KvCoder.of(NullableCoder.of(StringUtf8Coder.of()), VarIntCoder.of())));

    // GroupByEncryptedKey expects Secret#getSecretBytes() to return Base64-URL encoded
    // bytes of a valid AES key (e.g. 32 bytes for AES-256).
    byte[] secretBytes = "test-encryption-key-secret-12345".getBytes(StandardCharsets.UTF_8);
    Secret rawSecret = new RawSecret(Base64.getUrlEncoder().encode(secretBytes));
    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByEncryptedKey.<String, Integer>create(rawSecret));

    PAssert.that(output.apply("Sort", MapElements.via(new SortValues())))
        .containsInAnyOrder(
            KV.of("k1", Arrays.asList(3, 4)),
            KV.of(null, Arrays.asList(3, 5)),
            KV.of("k5", Arrays.asList(Integer.MIN_VALUE, Integer.MAX_VALUE)),
            KV.of("k2", Arrays.asList(-33, 66)),
            KV.of("k3", Arrays.asList(0)));

    p.run();
  }

  private static class SortValues
      extends SimpleFunction<KV<String, Iterable<Integer>>, KV<String, List<Integer>>> {
    @Override
    public KV<String, List<Integer>> apply(KV<String, Iterable<Integer>> input) {
      List<Integer> sorted =
          StreamSupport.stream(input.getValue().spliterator(), false)
              .sorted()
              .collect(Collectors.toList());
      return KV.of(input.getKey(), sorted);
    }
  }
}
