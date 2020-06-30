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
package org.apache.beam.sdk.io.aws.sts;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 *
 * {@link AWSCredentialsProvider} that loads credentials using Assume Role
 *
 */

public class STSCredentialsProviderWrapper implements AWSCredentialsProvider {

  private static final Log LOG = LogFactory.getLog(STSCredentialsProviderWrapper.class);
  public static final int DEFAULT_DURATION_SECONDS = 3600;
  public final String roleArn;
  public final String roleSessionName;
  public final String region;
  private final Regions rgn;
  private final AWSCredentialsProvider provider;

  public STSCredentialsProviderWrapper(String roleArn, String region, String roleSessionName) {
    this.roleArn = roleArn;
    this.region = region;
    this.roleSessionName = roleSessionName;
    rgn = region == null ? Regions.DEFAULT_REGION : Regions.fromName(region);
    provider = initializeProvider();
  }

  private AWSCredentialsProvider initializeProvider() {
    AWSSecurityTokenService stsClient =
        AWSSecurityTokenServiceClientBuilder.standard().withRegion(rgn).build();
    return new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
        .withStsClient(stsClient)
        .withRoleSessionDurationSeconds(DEFAULT_DURATION_SECONDS)
        .build();
  }

  @Override
  public AWSCredentials getCredentials() {
    return provider.getCredentials();
  }

  @Override
  public void refresh() {
    provider.refresh();
  }
}
