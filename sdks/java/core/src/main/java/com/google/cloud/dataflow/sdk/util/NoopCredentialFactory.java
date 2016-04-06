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
package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Construct an oauth credential to be used by the SDK and the SDK workers.
 * Always returns a null Credential object.
 */
public class NoopCredentialFactory implements CredentialFactory {
  public static NoopCredentialFactory fromOptions(PipelineOptions options) {
    return new NoopCredentialFactory();
  }

  @Override
  public Credential getCredential() throws IOException, GeneralSecurityException {
    return null;
  }
}
