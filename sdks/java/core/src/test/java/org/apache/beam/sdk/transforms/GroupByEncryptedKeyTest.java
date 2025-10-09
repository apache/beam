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

import static org.junit.Assert.assertThrows;

import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.GcpSecret;
import org.apache.beam.sdk.util.Secret;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GroupByEncryptedKey}. */
@RunWith(JUnit4.class)
public class GroupByEncryptedKeyTest implements Serializable {

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static class FakeSecret implements Secret {
    private final byte[] secret =
        "aKwI2PmqYFt2p5tNKCyBS5qYmHhHsGZc".getBytes(Charset.defaultCharset());

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

  private static final String PROJECT_ID = "apache-beam-testing";
  private static final String SECRET_ID = "gbek-test";
  private static Secret gcpSecret;

  @BeforeClass
  public static void setup() throws IOException {
    SecretManagerServiceClient client = SecretManagerServiceClient.create();
    ProjectName projectName = ProjectName.of(PROJECT_ID);
    SecretName secretName = SecretName.of(PROJECT_ID, SECRET_ID);

    try {
      client.getSecret(secretName);
    } catch (Exception e) {
      com.google.cloud.secretmanager.v1.Secret secret =
          com.google.cloud.secretmanager.v1.Secret.newBuilder()
              .setReplication(
                  com.google.cloud.secretmanager.v1.Replication.newBuilder()
                      .setAutomatic(
                          com.google.cloud.secretmanager.v1.Replication.Automatic.newBuilder()
                              .build())
                      .build())
              .build();
      client.createSecret(projectName, SECRET_ID, secret);
      byte[] secretBytes = new byte[32];
      new SecureRandom().nextBytes(secretBytes);
      client.addSecretVersion(
          secretName, SecretPayload.newBuilder().setData(ByteString.copyFrom(secretBytes)).build());
    }
    gcpSecret = new GcpSecret(secretName.toString() + "/versions/latest");
  }

  @AfterClass
  public static void tearDown() throws IOException {
    SecretManagerServiceClient client = SecretManagerServiceClient.create();
    SecretName secretName = SecretName.of(PROJECT_ID, SECRET_ID);
    client.deleteSecret(secretName);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByKeyGcpSecret() {
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
        input.apply(GroupByEncryptedKey.<String, Integer>create(gcpSecret));

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
  public void testGroupByKeyGcpSecretThrows() {
    Secret gcpSecret = new GcpSecret("bad_path/versions/latest");
    p.apply(Create.of(KV.of("k1", 1)))
        .apply(GroupByEncryptedKey.<String, Integer>create(gcpSecret));
    assertThrows(RuntimeException.class, () -> p.run());
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
