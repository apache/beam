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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.DefaultClientBuilder;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@RunWith(MockitoJUnitRunner.class)
public class DefaultS3ClientBuilderFactoryTest {
  @Mock S3Options s3Options;
  @Mock S3ClientBuilder builder;

  @Before
  public void prepareOptions() {
    when(s3Options.getClientBuilderFactory()).thenReturn((Class) DefaultClientBuilder.class);
  }

  @Test
  public void testEmptyOptions() {
    DefaultS3ClientBuilderFactory.createBuilder(builder, s3Options);
    verifyNoInteractions(builder);
  }

  @Test
  public void testSetCredentialsProvider() {
    AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
    when(s3Options.getAwsCredentialsProvider()).thenReturn(credentialsProvider);

    DefaultS3ClientBuilderFactory.createBuilder(builder, s3Options);
    verify(builder).credentialsProvider(credentialsProvider);
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testSetProxyConfiguration() {
    when(s3Options.getProxyConfiguration()).thenReturn(ProxyConfiguration.builder().build());

    DefaultS3ClientBuilderFactory.createBuilder(builder, s3Options);
    verify(builder).httpClientBuilder(any(ApacheHttpClient.Builder.class));
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testSetEndpoint() {
    URI endpointOverride = URI.create("https://localhost");
    when(s3Options.getEndpoint()).thenReturn(endpointOverride);
    DefaultS3ClientBuilderFactory.createBuilder(builder, s3Options);
    verify(builder).endpointOverride(endpointOverride);
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testSetRegion() {
    when(s3Options.getAwsRegion()).thenReturn(Region.US_WEST_1);
    DefaultS3ClientBuilderFactory.createBuilder(builder, s3Options);
    verify(builder).region(Region.US_WEST_1);
    verifyNoMoreInteractions(builder);
  }
}
