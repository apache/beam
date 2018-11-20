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
package org.apache.beam.sdk.extensions.gcp.auth;

import com.google.auth.Credentials;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Construct an oauth credential to be used by the SDK and the SDK workers. Always returns a null
 * Credential object.
 */
public class NoopCredentialFactory implements CredentialFactory {
  private static final NoopCredentialFactory INSTANCE = new NoopCredentialFactory();
  private static final NoopCredentials NOOP_CREDENTIALS = new NoopCredentials();

  public static NoopCredentialFactory fromOptions(PipelineOptions options) {
    return INSTANCE;
  }

  @Override
  public Credentials getCredential() throws IOException {
    return NOOP_CREDENTIALS;
  }

  private static class NoopCredentials extends Credentials {
    @Override
    public String getAuthenticationType() {
      return null;
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
      return null;
    }

    @Override
    public boolean hasRequestMetadata() {
      return false;
    }

    @Override
    public boolean hasRequestMetadataOnly() {
      return false;
    }

    @Override
    public void refresh() throws IOException {}
  }
}
