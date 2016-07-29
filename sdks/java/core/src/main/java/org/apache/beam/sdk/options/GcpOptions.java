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
package org.apache.beam.sdk.options;

import org.apache.beam.sdk.util.CredentialFactory;
import org.apache.beam.sdk.util.GcpCredentialFactory;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.PathValidator;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleOAuthConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.Files;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Options used to configure Google Cloud Platform project and credentials.
 *
 * <p>These options configure which of the following three different mechanisms for obtaining a
 * credential are used:
 * <ol>
 *   <li>
 *     It can fetch the
 *     <a href="https://developers.google.com/accounts/docs/application-default-credentials">
 *     application default credentials</a>.
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
 *
 * <p>The default mechanism is to use the
 * <a href="https://developers.google.com/accounts/docs/application-default-credentials">
 * application default credentials</a>. The other options can be
 * used by setting the corresponding properties.
 */
@Description("Options used to configure Google Cloud Platform project and credentials.")
public interface GcpOptions extends GoogleApiDebugOptions, PipelineOptions {
  /**
   * Project id to use when launching jobs.
   */
  @Description("Project id. Required when running a Dataflow in the cloud. "
      + "See https://cloud.google.com/storage/docs/projects for further details.")
  @Default.InstanceFactory(DefaultProjectFactory.class)
  String getProject();
  void setProject(String value);

  /**
   * This option controls which file to use when attempting to create the credentials using the
   * service account method.
   *
   * <p>This option if specified, needs be combined with the
   * {@link GcpOptions#getServiceAccountName() serviceAccountName}.
   */
  @JsonIgnore
  @Description("Controls which file to use when attempting to create the credentials "
      + "using the service account method. This option if specified, needs to be combined with "
      + "the serviceAccountName option.")
  String getServiceAccountKeyfile();
  void setServiceAccountKeyfile(String value);

  /**
   * This option controls which service account to use when attempting to create the credentials
   * using the service account method.
   *
   * <p>This option if specified, needs be combined with the
   * {@link GcpOptions#getServiceAccountKeyfile() serviceAccountKeyfile}.
   */
  @JsonIgnore
  @Description("Controls which service account to use when attempting to create the credentials "
      + "using the service account method. This option if specified, needs to be combined with "
      + "the serviceAccountKeyfile option.")
  String getServiceAccountName();
  void setServiceAccountName(String value);

  /**
   * This option controls which file to use when attempting to create the credentials
   * using the OAuth 2 webflow. After the OAuth2 webflow, the credentials will be stored
   * within credentialDir.
   */
  @JsonIgnore
  @Description("This option controls which file to use when attempting to create the credentials "
      + "using the OAuth 2 webflow. After the OAuth2 webflow, the credentials will be stored "
      + "within credentialDir.")
  String getSecretsFile();
  void setSecretsFile(String value);

  /**
   * This option controls which credential store to use when creating the credentials
   * using the OAuth 2 webflow.
   */
  @Description("This option controls which credential store to use when creating the credentials "
      + "using the OAuth 2 webflow.")
  @Default.String("cloud_dataflow")
  String getCredentialId();
  void setCredentialId(String value);

  /**
   * Directory for storing dataflow credentials after execution of the OAuth 2 webflow. Defaults
   * to using the $HOME/.store/data-flow directory.
   */
  @Description("Directory for storing dataflow credentials after execution of the OAuth 2 webflow. "
      + "Defaults to using the $HOME/.store/data-flow directory.")
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

  /**
   * The class of the credential factory that should be created and used to create
   * credentials. If gcpCredential has not been set explicitly, an instance of this class will
   * be constructed and used as a credential factory.
   */
  @Description("The class of the credential factory that should be created and used to create "
      + "credentials. If gcpCredential has not been set explicitly, an instance of this class will "
      + "be constructed and used as a credential factory.")
  @Default.Class(GcpCredentialFactory.class)
  Class<? extends CredentialFactory> getCredentialFactoryClass();
  void setCredentialFactoryClass(
      Class<? extends CredentialFactory> credentialFactoryClass);

  /**
   * The credential instance that should be used to authenticate against GCP services.
   * If no credential has been set explicitly, the default is to use the instance factory
   * that constructs a credential based upon the currently set credentialFactoryClass.
   */
  @JsonIgnore
  @Description("The credential instance that should be used to authenticate against GCP services. "
      + "If no credential has been set explicitly, the default is to use the instance factory "
      + "that constructs a credential based upon the currently set credentialFactoryClass.")
  @Default.InstanceFactory(GcpUserCredentialsFactory.class)
  @Hidden
  Credential getGcpCredential();
  void setGcpCredential(Credential value);

  /**
   * Attempts to infer the default project based upon the environment this application
   * is executing within. Currently this only supports getting the default project from gcloud.
   */
  public static class DefaultProjectFactory implements DefaultValueFactory<String> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultProjectFactory.class);

    @Override
    public String create(PipelineOptions options) {
      try {
        File configFile;
        if (getEnvironment().containsKey("CLOUDSDK_CONFIG")) {
          configFile = new File(getEnvironment().get("CLOUDSDK_CONFIG"), "properties");
        } else if (isWindows() && getEnvironment().containsKey("APPDATA")) {
          configFile = new File(getEnvironment().get("APPDATA"), "gcloud/properties");
        } else {
          // New versions of gcloud use this file
          configFile = new File(
              System.getProperty("user.home"),
              ".config/gcloud/configurations/config_default");
          if (!configFile.exists()) {
            // Old versions of gcloud use this file
            configFile = new File(System.getProperty("user.home"), ".config/gcloud/properties");
          }
        }
        String section = null;
        Pattern projectPattern = Pattern.compile("^project\\s*=\\s*(.*)$");
        Pattern sectionPattern = Pattern.compile("^\\[(.*)\\]$");
        for (String line : Files.readLines(configFile, StandardCharsets.UTF_8)) {
          line = line.trim();
          if (line.isEmpty() || line.startsWith(";")) {
            continue;
          }
          Matcher matcher = sectionPattern.matcher(line);
          if (matcher.matches()) {
            section = matcher.group(1);
          } else if (section == null || section.equals("core")) {
            matcher = projectPattern.matcher(line);
            if (matcher.matches()) {
              String project = matcher.group(1).trim();
              LOG.info("Inferred default GCP project '{}' from gcloud. If this is the incorrect "
                  + "project, please cancel this Pipeline and specify the command-line "
                  + "argument --project.", project);
              return project;
            }
          }
        }
      } catch (IOException expected) {
        LOG.debug("Failed to find default project.", expected);
      }
      // return null if can't determine
      return null;
    }

    /**
     * Returns true if running on the Windows OS.
     */
    private static boolean isWindows() {
      return System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("windows");
    }

    /**
     * Used to mock out getting environment variables.
     */
    @VisibleForTesting
    Map<String, String> getEnvironment() {
        return System.getenv();
    }
  }

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

  /**
   * The token server URL to use for OAuth 2 authentication. Normally, the default is sufficient,
   * but some specialized use cases may want to override this value.
   */
  @Description("The token server URL to use for OAuth 2 authentication. Normally, the default "
      + "is sufficient, but some specialized use cases may want to override this value.")
  @Default.String(GoogleOAuthConstants.TOKEN_SERVER_URL)
  @Hidden
  String getTokenServerUrl();
  void setTokenServerUrl(String value);

  /**
   * The authorization server URL to use for OAuth 2 authentication. Normally, the default is
   * sufficient, but some specialized use cases may want to override this value.
   */
  @Description("The authorization server URL to use for OAuth 2 authentication. Normally, the "
      + "default is sufficient, but some specialized use cases may want to override this value.")
  @Default.String(GoogleOAuthConstants.AUTHORIZATION_SERVER_URL)
  @Hidden
  String getAuthorizationServerEncodedUrl();
  void setAuthorizationServerEncodedUrl(String value);

  /**
   * A GCS path for storing temporary files in GCP.
   *
   * <p>Its default to {@link PipelineOptions#getTempLocation}.
   */
  @Description("A GCS path for storing temporary files in GCP.")
  @Default.InstanceFactory(GcpTempLocationFactory.class)
  @Nullable String getGcpTempLocation();
  void setGcpTempLocation(String value);

  /**
   * Returns {@link PipelineOptions#getTempLocation} as the default GCP temp location.
   */
  public static class GcpTempLocationFactory implements DefaultValueFactory<String> {

    @Override
    @Nullable
    public String create(PipelineOptions options) {
      String tempLocation = options.getTempLocation();
      if (!Strings.isNullOrEmpty(tempLocation)) {
        try {
          PathValidator validator = options.as(GcsOptions.class).getPathValidator();
          System.err.println(validator);
          validator.validateOutputFilePrefixSupported(tempLocation);
        } catch (Exception e) {
          // Ignore the temp location because it is not a valid 'gs://' path.
          return null;
        }
      }
      return tempLocation;
    }
  }
}
