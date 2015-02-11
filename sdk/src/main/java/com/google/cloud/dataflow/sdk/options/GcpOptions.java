/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.dataflow.sdk.util.CredentialFactory;
import com.google.cloud.dataflow.sdk.util.GcpCredentialFactory;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Options used to configure Google Cloud Platform project and credentials.
 * <p>
 * These options configure which of the following 4 different mechanisms for obtaining a credential
 * are used:
 * <ol>
 *   <li>
 *     It can fetch the
 *     <a href="https://developers.google.com/accounts/docs/application-default-credentials">
 *     application default credentials</a>.
 *   </li>
 *   <li>
 *     It can run the gcloud tool in a subprocess to obtain a credential.
 *     This is the preferred mechanism.  The property "GCloudPath" can be
 *     used to specify where we search for gcloud data.
 *   </li>
 *   <li>
 *     The user can specify a client secrets file and go through the OAuth2
 *     webflow. The credential will then be cached in the user's home
 *     directory for reuse.
 *   </li>
 *   <li>
 *     The user can specify a file containing a service account private key along
 *     with the service account name.
 *   </li>
 * </ol>
 * The default mechanism is to use the
 * <a href="https://developers.google.com/accounts/docs/application-default-credentials">
 * application default credentials</a> falling back to gcloud. The other options can be
 * used by setting the corresponding properties.
 */
public interface GcpOptions extends GoogleApiDebugOptions, PipelineOptions {
  /**
   * Project id to use when launching jobs.
   */
  @Description("Project id.  Required when running a Dataflow in the cloud.")
  String getProject();
  void setProject(String value);

  /**
   * This option controls which file to use when attempting to create the credentials using the
   * OAuth 2 webflow.
   */
  @Description("Path to a file containing Google API secret")
  String getSecretsFile();
  void setSecretsFile(String value);

  /**
   * This option controls which file to use when attempting to create the credentials using the
   * service account method.
   * <p>
   * This option if specified, needs be combined with the
   * {@link GcpOptions#getServiceAccountName() serviceAccountName}.
   */
  @Description("Path to a file containing the P12 service credentials")
  String getServiceAccountKeyfile();
  void setServiceAccountKeyfile(String value);

  /**
   * This option controls which service account to use when attempting to create the credentials
   * using the service account method.
   * <p>
   * This option if specified, needs be combined with the
   * {@link GcpOptions#getServiceAccountKeyfile() serviceAccountKeyfile}.
   */
  @Description("Name of the service account for Google APIs")
  String getServiceAccountName();
  void setServiceAccountName(String value);

  @Description("The path to the gcloud binary. "
      + " Default is to search the system path.")
  String getGCloudPath();
  void setGCloudPath(String value);

  /**
   * Directory for storing dataflow credentials.
   */
  @Description("Directory for storing dataflow credentials")
  @Default.InstanceFactory(CredentialDirFactory.class)
  String getCredentialDir();
  void setCredentialDir(String value);

  /**
   * Returns the default credential directory of ${user.home}/.store/data-flow.
   */
  public static class CredentialDirFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      File home = new File(System.getProperty("user.home"));
      File store = new File(home, ".store");
      File dataflow = new File(store, "data-flow");
      return dataflow.getPath();
    }
  }

  @Description("The credential identifier when using a persistent"
      + " credential store")
  @Default.String("cloud_dataflow")
  String getCredentialId();
  void setCredentialId(String value);

  @Description("The factory class used to create oauth credentials")
  @Default.Class(GcpCredentialFactory.class)
  Class<? extends CredentialFactory> getCredentialFactoryClass();
  void setCredentialFactoryClass(
      Class<? extends CredentialFactory> credentialFactoryClass);

  /** Alternative Google Cloud Platform Credential. */
  @JsonIgnore
  @Description("Google Cloud Platform user credentials.")
  @Default.InstanceFactory(GcpUserCredentialsFactory.class)
  Credential getGcpCredential();
  void setGcpCredential(Credential value);

  /**
   * Attempts to load the GCP credentials. See
   * {@link CredentialFactory#getCredential()} for more details.
   */
  public static class GcpUserCredentialsFactory implements DefaultValueFactory<Credential> {
    @Override
    public Credential create(PipelineOptions options) {
      GcpOptions gcpOptions = options.as(GcpOptions.class);
      try {
        CredentialFactory factory = InstanceBuilder.ofType(CredentialFactory.class)
            .fromClass(gcpOptions.getCredentialFactoryClass())
            .fromFactoryMethod("fromOptions")
            .withArg(PipelineOptions.class, options)
            .build();
        return factory.getCredential();
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException("Unable to obtain credential", e);
      }
    }
  }
}
