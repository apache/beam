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
package org.apache.beam.it.gcp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.it.common.TestProperties;
import org.junit.Before;

@SuppressWarnings("nullness")
public abstract class GCPBaseIT {
  protected static final String PROJECT = TestProperties.project();
  protected static final String REGION = TestProperties.region();

  protected Credentials credentials;

  @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  protected CredentialsProvider credentialsProvider;

  @Before
  public void setUpBase() {
    if (TestProperties.hasAccessToken()) {
      credentials = TestProperties.googleCredentials();
    } else {
      credentials = TestProperties.buildCredentialsFromEnv();
    }
    credentialsProvider = FixedCredentialsProvider.create(credentials);
  }
}
