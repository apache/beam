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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AbstractPromptReceiver;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleOAuthConstants;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.options.GcpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides support for loading credentials.
 */
public class Credentials {

  private static final Logger LOG = LoggerFactory.getLogger(Credentials.class);

  /**
   * OAuth 2.0 scopes used by a local worker (not on GCE).
   * The scope cloud-platform provides access to all Cloud Platform resources.
   * cloud-platform isn't sufficient yet for talking to datastore so we request
   * those resources separately.
   *
   * <p>Note that trusted scope relationships don't apply to OAuth tokens, so for
   * services we access directly (GCS) as opposed to through the backend
   * (BigQuery, GCE), we need to explicitly request that scope.
   */
  private static final List<String> SCOPES = Arrays.asList(
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/devstorage.full_control",
      "https://www.googleapis.com/auth/userinfo.email",
      "https://www.googleapis.com/auth/datastore");

  private static class PromptReceiver extends AbstractPromptReceiver {
    @Override
    public String getRedirectUri() {
      return GoogleOAuthConstants.OOB_REDIRECT_URI;
    }
  }

  /**
   * Initializes OAuth2 credentials.
   *
   * <p>This can use 3 different mechanisms for obtaining a credential:
   * <ol>
   *   <li>
   *     It can fetch the
   *     <a href="https://developers.google.com/accounts/docs/application-default-credentials">
   *     application default credentials</a>.
   *   </li>
   *   <li>
   *     The user can specify a client secrets file and go through the OAuth2
   *     webflow. The credential will then be cached in the user's home
   *     directory for reuse. Provide the property "secrets_file" to use this
   *     mechanism.
   *   </li>
   *   <li>
   *     The user can specify a file containing a service account.
   *     Provide the properties "service_account_keyfile" and
   *     "service_account_name" to use this mechanism.
   *   </li>
   * </ol>
   * The default mechanism is to use the
   * <a href="https://developers.google.com/accounts/docs/application-default-credentials">
   * application default credentials</a>. The other options can be used by providing the
   * corresponding properties.
   */
  public static Credential getCredential(GcpOptions options)
      throws IOException, GeneralSecurityException {
    String keyFile = options.getServiceAccountKeyfile();
    String accountName = options.getServiceAccountName();

    if (keyFile != null && accountName != null) {
      try {
        return getCredentialFromFile(keyFile, accountName, SCOPES);
      } catch (GeneralSecurityException e) {
        throw new IOException("Unable to obtain credentials from file", e);
      }
    }

    if (options.getSecretsFile() != null) {
      return getCredentialFromClientSecrets(options, SCOPES);
    }

    try {
      return GoogleCredential.getApplicationDefault().createScoped(SCOPES);
    } catch (IOException e) {
      throw new RuntimeException("Unable to get application default credentials. Please see "
          + "https://developers.google.com/accounts/docs/application-default-credentials "
          + "for details on how to specify credentials. This version of the SDK is "
          + "dependent on the gcloud core component version 2015.02.05 or newer to "
          + "be able to get credentials from the currently authorized user via gcloud auth.", e);
    }
  }

  /**
   * Loads OAuth2 credential from a local file.
   */
  private static Credential getCredentialFromFile(
      String keyFile, String accountId, Collection<String> scopes)
      throws IOException, GeneralSecurityException {
    GoogleCredential credential = new GoogleCredential.Builder()
        .setTransport(Transport.getTransport())
        .setJsonFactory(Transport.getJsonFactory())
        .setServiceAccountId(accountId)
        .setServiceAccountScopes(scopes)
        .setServiceAccountPrivateKeyFromP12File(new File(keyFile))
        .build();

    LOG.info("Created credential from file {}", keyFile);
    return credential;
  }

  /**
   * Loads OAuth2 credential from client secrets, which may require an
   * interactive authorization prompt.
   */
  private static Credential getCredentialFromClientSecrets(
      GcpOptions options, Collection<String> scopes)
      throws IOException, GeneralSecurityException {
    String clientSecretsFile = options.getSecretsFile();

    checkArgument(clientSecretsFile != null);
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    GoogleClientSecrets clientSecrets;

    try {
      clientSecrets = GoogleClientSecrets.load(jsonFactory,
          new FileReader(clientSecretsFile));
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not read the client secrets from file: " + clientSecretsFile,
          e);
    }

    FileDataStoreFactory dataStoreFactory =
        new FileDataStoreFactory(new java.io.File(options.getCredentialDir()));

    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
        httpTransport, jsonFactory, clientSecrets, scopes)
        .setDataStoreFactory(dataStoreFactory)
        .setTokenServerUrl(new GenericUrl(options.getTokenServerUrl()))
        .setAuthorizationServerEncodedUrl(options.getAuthorizationServerEncodedUrl())
        .build();

    // The credentialId identifies the credential if we're using a persistent
    // credential store.
    Credential credential =
        new AuthorizationCodeInstalledApp(flow, new PromptReceiver())
            .authorize(options.getCredentialId());

    LOG.info("Got credential from client secret");
    return credential;
  }
}
