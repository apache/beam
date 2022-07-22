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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Construct an oauth credential to be used by the SDK and the SDK workers. Returns a GCP
 * credential.
 */
public class GcpCredentialFactory implements CredentialFactory {
  /**
   * The scope cloud-platform provides access to all Cloud Platform resources. cloud-platform isn't
   * sufficient yet for talking to datastore so we request those resources separately.
   *
   * <p>Note that trusted scope relationships don't apply to OAuth tokens, so for services we access
   * directly (GCS) as opposed to through the backend (BigQuery, GCE), we need to explicitly request
   * that scope.
   */
  private static final List<String> SCOPES =
      Arrays.asList(
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/devstorage.full_control",
          "https://www.googleapis.com/auth/userinfo.email",
          "https://www.googleapis.com/auth/datastore",
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/bigquery.insertdata",
          "https://www.googleapis.com/auth/pubsub");

  // If non-null, a list of service account emails to be used as an impersonation chain.
  private @Nullable List<String> impersonateServiceAccountChain;

  private GcpCredentialFactory(@Nullable List<String> impersonateServiceAccountChain) {
    if (impersonateServiceAccountChain != null) {
      checkArgument(impersonateServiceAccountChain.size() > 0);
    }

    this.impersonateServiceAccountChain = impersonateServiceAccountChain;
  }

  public static GcpCredentialFactory fromOptions(PipelineOptions options) {
    @Nullable
    String impersonateServiceAccountArg =
        options.as(GcpOptions.class).getImpersonateServiceAccount();

    @Nullable
    List<String> impersonateServiceAccountChain =
        impersonateServiceAccountArg == null
            ? null
            : Arrays.asList(impersonateServiceAccountArg.split(","));

    return new GcpCredentialFactory(impersonateServiceAccountChain);
  }

  /** Returns a default GCP {@link Credentials} or null when it fails. */
  @Override
  public @Nullable Credentials getCredential() {
    try {
      GoogleCredentials applicationDefaultCredentials =
          GoogleCredentials.getApplicationDefault().createScoped(SCOPES);

      if (impersonateServiceAccountChain == null) {
        return applicationDefaultCredentials;
      } else {
        String targetPrincipal =
            impersonateServiceAccountChain.get(impersonateServiceAccountChain.size() - 1);
        List<String> delegationChain =
            impersonateServiceAccountChain.subList(0, impersonateServiceAccountChain.size() - 1);

        GoogleCredentials impersonationCredentials =
            ImpersonatedCredentials.create(
                applicationDefaultCredentials, targetPrincipal, delegationChain, SCOPES, 0);

        return impersonationCredentials;
      }
    } catch (IOException e) {
      // Ignore the exception
      // Pipelines that only access to public data should be able to run without credentials.
      return null;
    }
  }
}
