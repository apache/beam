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
package org.apache.beam.sdk.io.aws2.s3;

import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws2.ITEnvironment;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow.DeterministicallyConstructTestRowFn;
import org.apache.beam.sdk.io.common.TestRow.SelectNameFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Integration test to write and read from a S3 compatible file system.
 *
 * <p>By default this runs against Localstack, but you can use {@link S3FileSystemIT.S3ITOptions} to
 * configure tests to run against AWS S3.
 *
 * <pre>{@code
 * ./gradlew :sdks:java:io:amazon-web-services2:integrationTest \
 *   --info \
 *   --tests "org.apache.beam.sdk.io.aws2.s3.S3FileSystemIT" \
 *   -DintegrationTestPipelineOptions='["--awsRegion=eu-central-1","--useLocalstack=false"]'
 * }</pre>
 */
@RunWith(JUnit4.class)
public class S3FileSystemIT {
  public interface S3ITOptions extends ITEnvironment.ITOptions, S3Options {}

  @ClassRule
  public static ITEnvironment<S3ITOptions> env =
      new ITEnvironment<>(S3, S3ITOptions.class, S3ClientFixFix::set);

  @Rule public TestPipeline pipelineWrite = env.createTestPipeline();
  @Rule public TestPipeline pipelineRead = env.createTestPipeline();
  @Rule public S3Bucket s3Bucket = new S3Bucket();

  @Test
  public void testWriteThenRead() {
    int rows = env.options().getNumberOfRows();
    // Write test dataset to S3.
    pipelineWrite
        .apply("Generate Sequence", GenerateSequence.from(0).to(rows))
        .apply("Prepare TestRows", ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply("Prepare file rows", ParDo.of(new SelectNameFn()))
        .apply("Write to S3 file", TextIO.write().to("s3://" + s3Bucket.name + "/test"));

    pipelineWrite.run().waitUntilFinish();

    // Read test dataset from S3.
    PCollection<String> output =
        pipelineRead.apply(TextIO.read().from("s3://" + s3Bucket.name + "/test*"));

    PAssert.thatSingleton(output.apply(Count.globally())).isEqualTo((long) rows);

    PAssert.that(output.apply(Combine.globally(new HashingFn()).withoutDefaults()))
        .containsInAnyOrder(getExpectedHashForRowCount(rows));

    pipelineRead.run().waitUntilFinish();
  }

  static class S3Bucket extends ExternalResource {
    public final String name = "beam-s3io-it-" + new DateTime().toString("yyyyMMdd-HHmmss");

    @Override
    protected void before() throws Throwable {
      try (S3Client client = env.buildClient(S3Client.builder())) {
        client.createBucket(b -> b.bucket(name));
      }
    }
  }

  // Disable chunkedEncoding to prevent localstack bug, see
  // https://github.com/localstack/localstack/issues/4987
  private static class S3ClientFixFix extends DefaultS3ClientBuilderFactory {
    private static void set(S3Options s3Options) {
      s3Options.setS3ClientFactoryClass(S3ClientFixFix.class);
    }

    @Override
    public S3ClientBuilder createBuilder(S3Options s3Options) {
      return super.createBuilder(s3Options)
          .serviceConfiguration(c -> c.chunkedEncodingEnabled(false));
    }
  }
}
