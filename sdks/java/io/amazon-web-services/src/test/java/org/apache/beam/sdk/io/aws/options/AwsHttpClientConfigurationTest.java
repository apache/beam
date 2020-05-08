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
package org.apache.beam.sdk.io.aws.options;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * {@link AwsHttpClientConfigurationTest}. Test to verify that aws http client configuration are
 * correctly being set for the respective AWS services.
 */
@RunWith(JUnit4.class)
public class AwsHttpClientConfigurationTest {

  @Test
  public void testAwsHttpClientConfigurationValues() {
    S3Options s3Options = getOptions();
    assertEquals(5000, s3Options.getClientConfiguration().getSocketTimeout());
    assertEquals(1000, s3Options.getClientConfiguration().getClientExecutionTimeout());
    assertEquals(10, s3Options.getClientConfiguration().getMaxConnections());
  }

  private static S3Options getOptions() {
    String[] args = {
      "--s3ClientFactoryClass=org.apache.beam.sdk.io.aws.s3.DefaultS3ClientBuilderFactory",
      "--clientConfiguration={\"clientExecutionTimeout\":1000,"
          + "\"maxConnections\":10,"
          + "\"socketTimeout\":5000}"
    };
    return PipelineOptionsFactory.fromArgs(args).as(S3Options.class);
  }
}
