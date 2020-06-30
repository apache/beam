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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 *
 *  {@link STSAssumeRoleSessionCredentialsProvider STSAssumeRoleSessionCredentialsProvider}.
 *
 */
public class AssumeRoleSessionCredentialsProvider implements AWSCredentialsProvider {

  private static final Log LOG = LogFactory.getLog(AssumeRoleSessionCredentialsProvider.class);
  private STSAssumeRoleSessionCredentialsProvider sessionCredentialsProvider;

  private AssumeRoleSessionCredentialsProvider(String assumeRoleARN, String sessionName) {
    LOG.info("Getting Session Credentials!");
    sessionCredentialsProvider =
        new STSAssumeRoleSessionCredentialsProvider.Builder(assumeRoleARN, sessionName).build();
  }

  public static AssumeRoleSessionCredentialsProvider getInstance(
      String assumeRoleARN, String sessionName) {
    if (StringUtils.isBlank(assumeRoleARN)) {
      LOG.error("assume role ARN is empty!");
      throw new IllegalArgumentException("assume role ARN is empty");
    }
    if (StringUtils.isBlank(sessionName)) {
      LOG.error("session name is empty!");
      throw new IllegalArgumentException("session name is empty");
    }

    return new AssumeRoleSessionCredentialsProvider(assumeRoleARN, sessionName);
  }

  public STSAssumeRoleSessionCredentialsProvider getSessionCredentialsProvider() {
    return this.sessionCredentialsProvider;
  }

  @Override
  public AWSCredentials getCredentials() {
    return getSessionCredentialsProvider().getCredentials();
  }

  @Override
  public void refresh() {
    getSessionCredentialsProvider().refresh();
  }
}
