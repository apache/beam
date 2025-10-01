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
package org.apache.beam.runners.dataflow.transforms;

import com.google.api.services.dataflow.Dataflow;
import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link GroupByKey} for the {@link DataflowRunner}. */
@RunWith(JUnit4.class)
public class DataflowGroupByKeyTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private Dataflow dataflow;

  private static final String PROJECT_ID = "apache-beam-testing";
  private static final String SECRET_ID = "gbek-test";
  private static String gcpSecretVersionName;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

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
    gcpSecretVersionName = secretName.toString() + "/versions/latest";
  }

  @AfterClass
  public static void tearDown() throws IOException {
    SecretManagerServiceClient client = SecretManagerServiceClient.create();
    SecretName secretName = SecretName.of(PROJECT_ID, SECRET_ID);
    client.deleteSecret(secretName);
  }

  /**
   * Create a test pipeline that uses the {@link DataflowRunner} so that {@link GroupByKey} is not
   * expanded. This is used for verifying that even without expansion the proper errors show up.
   */
  private Pipeline createTestServiceRunner() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("someproject");
    options.setRegion("some-region1");
    options.setGcpTempLocation("gs://staging");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setDataflowClient(dataflow);
    return Pipeline.create(options);
  }

  @Test
  public void testGroupByKeyServiceUnbounded() {
    Pipeline p = createTestServiceRunner();

    PCollection<KV<String, Integer>> input =
        p.apply(
            new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
              @Override
              public PCollection<KV<String, Integer>> expand(PBegin input) {
                return PCollection.createPrimitiveOutputInternal(
                    input.getPipeline(),
                    WindowingStrategy.globalDefault(),
                    PCollection.IsBounded.UNBOUNDED,
                    KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
              }
            });

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "GroupByKey cannot be applied to non-bounded PCollection in the GlobalWindow without "
            + "a trigger. Use a Window.into or Window.triggering transform prior to GroupByKey.");

    input.apply("GroupByKey", GroupByKey.create());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByKeyWithValidGcpSecretOption() {
    Pipeline p = createTestServiceRunner();
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

    p.getOptions().setGBEK(String.format("type:gcpsecret;version_name:%s", gcpSecretVersionName));
    PCollection<KV<String, Iterable<Integer>>> output = input.apply(new DataflowGroupByKey<>());

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("k1", Arrays.asList(3, 4)),
            KV.of("k5", Arrays.asList(Integer.MAX_VALUE, Integer.MIN_VALUE)),
            KV.of("k2", Arrays.asList(66, -33)),
            KV.of("k3", Arrays.asList(0)));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByKeyWithInvalidGcpSecretOption() {
    Pipeline p = createTestServiceRunner();
    p.getOptions().setGBEK("type:gcpsecret;version_name:bad_path/versions/latest");
    p.apply(Create.of(KV.of("k1", 1))).apply(new DataflowGroupByKey<>());
    thrown.expect(RuntimeException.class);
    p.run();
  }
}
