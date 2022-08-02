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
package org.apache.beam.sdk.io.aws2.options;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.beam.sdk.io.aws2.s3.DefaultS3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.s3.SSECustomerKey;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class S3OptionsTest {

  private S3Options serializeDeserialize(S3Options opts) {
    return SerializationTestUtil.serializeDeserialize(PipelineOptions.class, opts)
        .as(S3Options.class);
  }

  private S3Options create(String... args) {
    return PipelineOptionsFactory.fromArgs(args).as(S3Options.class);
  }

  @Test
  public void testSerializeDeserializeDefaults() {
    S3Options options = create();

    // trigger factories
    options.getSSECustomerKey();
    options.getS3UploadBufferSizeBytes();

    S3Options copy = serializeDeserialize(options);
    assertThat(copy.getS3StorageClass()).isEqualTo(options.getS3StorageClass());
    assertThat(copy.getS3UploadBufferSizeBytes()).isEqualTo(options.getS3UploadBufferSizeBytes());
    assertThat(copy.getS3ThreadPoolSize()).isEqualTo(options.getS3ThreadPoolSize());
    assertThat(copy.getSSECustomerKey())
        .isEqualToComparingFieldByField(options.getSSECustomerKey());
    assertThat(copy.getS3ClientFactoryClass()).isEqualTo(options.getS3ClientFactoryClass());

    assertThat(copy.getSSEKMSKeyId()).isNull();
    assertThat(options.getSSEKMSKeyId()).isNull();

    assertThat(copy.getSSEAlgorithm()).isNull();
    assertThat(options.getSSEAlgorithm()).isNull();
  }

  @Test
  public void testSetS3StorageClass() {
    S3Options options = create("--s3StorageClass=GLACIER");
    assertThat(options.getS3StorageClass()).isEqualTo("GLACIER");
    assertThat(serializeDeserialize(options).getS3StorageClass()).isEqualTo("GLACIER");
  }

  @Test
  public void testSetS3UploadBufferSizeBytes() {
    S3Options options = create("--s3UploadBufferSizeBytes=1024");
    assertThat(options.getS3UploadBufferSizeBytes()).isEqualTo(1024);
    assertThat(serializeDeserialize(options).getS3UploadBufferSizeBytes()).isEqualTo(1024);
  }

  @Test
  public void testSetS3ThreadPoolSize() {
    S3Options options = create("--s3ThreadPoolSize=10");
    assertThat(options.getS3ThreadPoolSize()).isEqualTo(10);
    assertThat(serializeDeserialize(options).getS3ThreadPoolSize()).isEqualTo(10);
  }

  @Test
  public void testSetSSEAlgorithm() {
    S3Options options = create("--SSEAlgorithm=AES256");
    assertThat(options.getSSEAlgorithm()).isEqualTo("AES256");
    assertThat(serializeDeserialize(options).getSSEAlgorithm()).isEqualTo("AES256");
  }

  @Test
  public void testSetSSECustomerKey() {
    S3Options options = create("--SSECustomerKey={\"key\": \"key\", \"algorithm\": \"algo\"}");

    SSECustomerKey expected = SSECustomerKey.builder().key("key").algorithm("algo").build();
    assertThat(options.getSSECustomerKey()).isEqualToComparingFieldByField(expected);

    assertThat(serializeDeserialize(options).getSSECustomerKey())
        .isEqualToComparingFieldByField(expected);
  }

  @Test
  public void testSetSSEKMSKeyId() {
    S3Options options = create("--SSEKMSKeyId=arnOfKey");
    assertThat(options.getSSEKMSKeyId()).isEqualTo("arnOfKey");
    assertThat(serializeDeserialize(options).getSSEKMSKeyId()).isEqualTo("arnOfKey");
  }

  @Test
  public void testSetS3ClientFactoryClass() {
    S3Options options =
        create("--s3ClientFactoryClass=" + NoopS3ClientBuilderFactory.class.getName());
    assertThat(options.getS3ClientFactoryClass()).isEqualTo(NoopS3ClientBuilderFactory.class);
    assertThat(serializeDeserialize(options).getS3ClientFactoryClass())
        .isEqualTo(NoopS3ClientBuilderFactory.class);
  }

  public static class NoopS3ClientBuilderFactory extends DefaultS3ClientBuilderFactory {}
}
