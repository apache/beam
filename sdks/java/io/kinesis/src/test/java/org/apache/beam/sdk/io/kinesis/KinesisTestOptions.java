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
package org.apache.beam.sdk.io.kinesis;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options for Kinesis integration tests. */
public interface KinesisTestOptions extends TestPipelineOptions {

  @Description("AWS region where Kinesis stream resided")
  @Default.String("aws-kinesis-region")
  String getAwsKinesisRegion();

  void setAwsKinesisRegion(String value);

  @Description("Kinesis stream name")
  @Default.String("aws-kinesis-stream")
  String getAwsKinesisStream();

  void setAwsKinesisStream(String value);

  @Description("AWS secret key")
  @Default.String("aws-secret-key")
  String getAwsSecretKey();

  void setAwsSecretKey(String value);

  @Description("AWS access key")
  @Default.String("aws-access-key")
  String getAwsAccessKey();

  void setAwsAccessKey(String value);

  @Description("Aws service endpoint")
  @Nullable
  String getAwsServiceEndpoint();

  void setAwsServiceEndpoint(String awsServiceEndpoint);

  @Description("Flag for certificate verification")
  @Default.Boolean(true)
  Boolean getAwsVerifyCertificate();

  void setAwsVerifyCertificate(Boolean awsVerifyCertificate);

  @Description("Number of shards of stream")
  @Default.Integer(2)
  Integer getNumberOfShards();

  void setNumberOfShards(Integer count);

  @Description("Number of records that will be written and read by the test")
  @Default.Integer(1000)
  Integer getNumberOfRecords();

  void setNumberOfRecords(Integer count);

  @Description("Use localstack. Disable to test with real Kinesis")
  @Default.Boolean(true)
  Boolean getUseLocalstack();

  void setUseLocalstack(Boolean useLocalstack);
}
