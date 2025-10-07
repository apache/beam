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

import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for GroupByKey transforms and some other transforms which use GBK. */
@RunWith(JUnit4.class)
public class GroupByKeyIT {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String PROJECT_ID = "apache-beam-testing";
  private static final String SECRET_ID = "gbek-test";
  private static String gcpSecretVersionName;
  private static String secretId;

  @BeforeClass
  public static void setup() throws IOException {
    secretId = String.format("%s-%d", SECRET_ID, new SecureRandom().nextInt(10000));
    SecretManagerServiceClient client;
    try {
      client = SecretManagerServiceClient.create();
    } catch (IOException e) {
      gcpSecretVersionName = null;
      return;
    }
    ProjectName projectName = ProjectName.of(PROJECT_ID);
    SecretName secretName = SecretName.of(PROJECT_ID, secretId);

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
      client.createSecret(projectName, secretId, secret);
      byte[] secretBytes = new byte[32];
      new SecureRandom().nextBytes(secretBytes);
      client.addSecretVersion(
          secretName,
          SecretPayload.newBuilder()
              .setData(ByteString.copyFrom(java.util.Base64.getEncoder().encode(secretBytes)))
              .build());
    }
    gcpSecretVersionName = secretName.toString() + "/versions/latest";
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (gcpSecretVersionName != null) {
      SecretManagerServiceClient client = SecretManagerServiceClient.create();
      SecretName secretName = SecretName.of(PROJECT_ID, secretId);
      client.deleteSecret(secretName);
    }
  }

  @Test
  public void testGroupByKeyWithValidGcpSecretOption() throws Exception {
    if (gcpSecretVersionName == null) {
      // Skip test if we couldn't set up secret manager
      return;
    }
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setGbek(String.format("type:gcpsecret;version_name:%s", gcpSecretVersionName));
    Pipeline p = Pipeline.create(options);
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

    PCollection<KV<String, Iterable<Integer>>> output = input.apply(GroupByKey.create());

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("k1", Arrays.asList(3, 4)),
            KV.of("k5", Arrays.asList(Integer.MAX_VALUE, Integer.MIN_VALUE)),
            KV.of("k2", Arrays.asList(66, -33)),
            KV.of("k3", Arrays.asList(0)));

    p.run();
  }

  @Test
  public void testGroupByKeyWithInvalidGcpSecretOption() throws Exception {
    if (gcpSecretVersionName == null) {
      // Skip test if we couldn't set up secret manager
      return;
    }
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setGbek("type:gcpsecret;version_name:bad_path/versions/latest");
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(KV.of("k1", 1))).apply(GroupByKey.create());
    thrown.expect(RuntimeException.class);
    p.run();
  }

  // Redistribute depends on GBK under the hood and can have runner-specific implementations
  @Test
  public void redistributeWithValidGcpSecretOption() throws Exception {
    if (gcpSecretVersionName == null) {
      // Skip test if we couldn't set up secret manager
      return;
    }
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setGbek(String.format("type:gcpsecret;version_name:%s", gcpSecretVersionName));
    Pipeline p = Pipeline.create(options);

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
    PCollection<KV<String, Integer>> output = input.apply(Redistribute.byKey());
    PAssert.that(output).containsInAnyOrder(ungroupedPairs);

    p.run();
  }

  @Test
  public void testRedistributeWithInvalidGcpSecretOption() throws Exception {
    if (gcpSecretVersionName == null) {
      // Skip test if we couldn't set up secret manager
      return;
    }
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setGbek("type:gcpsecret;version_name:bad_path/versions/latest");
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(KV.of("k1", 1))).apply(Redistribute.byKey());
    thrown.expect(RuntimeException.class);
    p.run();
  }
}
