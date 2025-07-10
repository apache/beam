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
package org.apache.beam.sdk.extensions.gcp.options;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Bucket.SoftDeletePolicy;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.GcpCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.storage.PathValidator;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options used to configure Google Cloud Platform specific options such as the project and
 * credentials.
 *
 * <p>These options defer to the <a
 * href="https://developers.google.com/accounts/docs/application-default-credentials">application
 * default credentials</a> for authentication. See the <a
 * href="https://github.com/google/google-auth-library-java">Google Auth Library</a> for alternative
 * mechanisms for creating credentials.
 */
@Description("Options used to configure Google Cloud Platform project and credentials.")
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public interface GcpOptions extends GoogleApiDebugOptions, PipelineOptions {
  /** Project id to use when launching jobs. */
  @Description(
      "Project id. Required when using Google Cloud Platform services. "
          + "See https://cloud.google.com/storage/docs/projects for further details.")
  @Default.InstanceFactory(DefaultProjectFactory.class)
  String getProject();

  void setProject(String value);

  /**
   * GCP <a href="https://developers.google.com/compute/docs/zones" >availability zone</a> for
   * operations.
   *
   * <p>Default is set on a per-service basis.
   *
   * @deprecated Use {@link #getWorkerZone()} instead.
   */
  @Deprecated
  @Description(
      "GCP availability zone for running GCP operations. "
          + "and GCE availability zone for launching workers "
          + "Default is up to the individual service. "
          + "This option is deprecated, and will be replaced by workerZone.")
  String getZone();

  /** @deprecated Use {@link #setWorkerZone} instead. */
  @Deprecated
  void setZone(String value);

  /**
   * The Compute Engine region (https://cloud.google.com/compute/docs/regions-zones/regions-zones)
   * in which worker processing should occur, e.g. "us-west1". Mutually exclusive with {@link
   * #getWorkerZone()}. If neither workerRegion nor workerZone is specified, default to same value
   * as region.
   */
  @Description(
      "The Compute Engine region "
          + "(https://cloud.google.com/compute/docs/regions-zones/regions-zones) in which worker "
          + "processing should occur, e.g. \"us-west1\". Mutually exclusive with workerZone. If "
          + "neither workerRegion nor workerZone is specified, default to same value as region.")
  String getWorkerRegion();

  void setWorkerRegion(String workerRegion);

  /**
   * The Compute Engine zone (https://cloud.google.com/compute/docs/regions-zones/regions-zones) in
   * which worker processing should occur, e.g. "us-west1-a". Mutually exclusive with {@link
   * #getWorkerRegion()}. If neither workerRegion nor workerZone is specified, the Dataflow service
   * will choose a zone in region based on available capacity.
   */
  @Description(
      "The Compute Engine zone "
          + "(https://cloud.google.com/compute/docs/regions-zones/regions-zones) in which worker "
          + "processing should occur, e.g. \"us-west1-a\". Mutually exclusive with workerRegion. "
          + "If neither workerRegion nor workerZone is specified, the Dataflow service will choose "
          + "a zone in region based on available capacity.")
  String getWorkerZone();

  void setWorkerZone(String workerZone);

  /**
   * Controls the OAuth scopes that will be requested when creating {@link Credentials} with the
   * {@link GcpCredentialFactory} (which is the default {@link CredentialFactory}). If the {@link
   * #setGcpCredential credential} or {@link #setCredentialFactoryClass credential factory} have
   * been set then this field may do nothing.
   */
  @Default.InstanceFactory(GcpOAuthScopesFactory.class)
  @Description(
      "Controls the OAuth scopes that will be requested when creating credentials with the GcpCredentialFactory (which is the default credential factory). If the GCP credential or credential factory have been set then this property may do nothing.")
  List<String> getGcpOauthScopes();

  void setGcpOauthScopes(List<String> oauthScopes);

  /** Returns the default set of OAuth scopes. */
  class GcpOAuthScopesFactory implements DefaultValueFactory<List<String>> {

    @Override
    public List<String> create(PipelineOptions options) {
      /**
       * The scope cloud-platform provides access to all Cloud Platform resources. cloud-platform
       * isn't sufficient yet for talking to datastore so we request those resources separately.
       *
       * <p>Note that trusted scope relationships don't apply to OAuth tokens, so for services we
       * access directly (GCS) as opposed to through the backend (BigQuery, GCE), we need to
       * explicitly request that scope.
       */
      return Arrays.asList(
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/devstorage.full_control",
          "https://www.googleapis.com/auth/userinfo.email",
          "https://www.googleapis.com/auth/datastore",
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/bigquery.insertdata",
          "https://www.googleapis.com/auth/pubsub");
    }
  }

  /**
   * The class of the credential factory that should be created and used to create credentials. If
   * gcpCredential has not been set explicitly, an instance of this class will be constructed and
   * used as a credential factory.
   */
  @Description(
      "The class of the credential factory that should be created and used to create "
          + "credentials. If gcpCredential has not been set explicitly, an instance of this class will "
          + "be constructed and used as a credential factory.")
  @Default.Class(GcpCredentialFactory.class)
  Class<? extends CredentialFactory> getCredentialFactoryClass();

  void setCredentialFactoryClass(Class<? extends CredentialFactory> credentialFactoryClass);

  /**
   * The credential instance that should be used to authenticate against GCP services. If no
   * credential has been set explicitly, the default is to use the instance factory that constructs
   * a credential based upon the currently set credentialFactoryClass.
   */
  @JsonIgnore
  @Description(
      "The credential instance that should be used to authenticate against GCP services. "
          + "If no credential has been set explicitly, the default is to use the instance factory "
          + "that constructs a credential based upon the currently set credentialFactoryClass.")
  @Default.InstanceFactory(GcpUserCredentialsFactory.class)
  Credentials getGcpCredential();

  void setGcpCredential(Credentials value);

  /**
   * All API requests will be made as the given service account or target service account in an
   * impersonation delegation chain instead of the currently selected account. You can specify
   * either a single service account as the impersonator, or a comma-separated list of service
   * accounts to create an impersonation delegation chain.
   */
  @Description(
      "All API requests will be made as the given service account or"
          + " target service account in an impersonation delegation chain"
          + " instead of the currently selected account. You can specify"
          + " either a single service account as the impersonator, or a"
          + " comma-separated list of service accounts to create an"
          + " impersonation delegation chain.")
  @Nullable
  String getImpersonateServiceAccount();

  void setImpersonateServiceAccount(String impersonateServiceAccount);

  /** Experiment to turn on the Streaming Engine experiment. */
  String STREAMING_ENGINE_EXPERIMENT = "enable_streaming_engine";

  /** @deprecated Use STREAMING_ENGINE_EXPERIMENT instead. */
  @Deprecated String WINDMILL_SERVICE_EXPERIMENT = "enable_windmill_service";

  @Description(
      "If true will use Streaming Engine.  Defaults to false unless the experiments enable_streaming_engine or enable_windmill_service are set.")
  @Default.InstanceFactory(EnableStreamingEngineFactory.class)
  boolean isEnableStreamingEngine();

  void setEnableStreamingEngine(boolean value);

  /**
   * Attempts to infer the default project based upon the environment this application is executing
   * within. Currently this only supports getting the active project from gcloud.
   */
  class DefaultProjectFactory implements DefaultValueFactory<String> {
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
          String activeConfig;
          File activeConfigFile =
              new File(System.getProperty("user.home"), ".config/gcloud/active_config");
          if (activeConfigFile.exists()) {
            activeConfig =
                Files.asCharSource(activeConfigFile, StandardCharsets.UTF_8).readFirstLine();
          } else {
            activeConfig = "default";
          }
          configFile =
              new File(
                  System.getProperty("user.home"),
                  ".config/gcloud/configurations/config_" + activeConfig);
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
          } else if (section == null || "core".equals(section)) {
            matcher = projectPattern.matcher(line);
            if (matcher.matches()) {
              String project = matcher.group(1).trim();
              LOG.info(
                  "Inferred default GCP project '{}' from gcloud. If this is the incorrect "
                      + "project, please cancel this Pipeline and specify the command-line "
                      + "argument --project.",
                  project);
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

    /** Returns true if running on the Windows OS. */
    private static boolean isWindows() {
      return System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("windows");
    }

    /** Used to mock out getting environment variables. */
    @VisibleForTesting
    Map<String, String> getEnvironment() {
      return System.getenv();
    }
  }

  /**
   * Attempts to load the GCP credentials. See {@link CredentialFactory#getCredential()} for more
   * details.
   */
  class GcpUserCredentialsFactory implements DefaultValueFactory<Credentials> {
    @Override
    public Credentials create(PipelineOptions options) {
      GcpOptions gcpOptions = options.as(GcpOptions.class);
      try {
        CredentialFactory factory =
            InstanceBuilder.ofType(CredentialFactory.class)
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

  /** EnableStreamingEngine defaults to false unless one of the two experiments is set. */
  class EnableStreamingEngineFactory implements DefaultValueFactory<Boolean> {
    @Override
    public Boolean create(PipelineOptions options) {
      return ExperimentalOptions.hasExperiment(options, STREAMING_ENGINE_EXPERIMENT)
          || ExperimentalOptions.hasExperiment(options, WINDMILL_SERVICE_EXPERIMENT);
    }
  }

  /**
   * A GCS path for storing temporary files in GCP.
   *
   * <p>Its default to {@link PipelineOptions#getTempLocation}.
   */
  @Description("A GCS path for storing temporary files in GCP.")
  @Default.InstanceFactory(GcpTempLocationFactory.class)
  @Nullable
  String getGcpTempLocation();

  void setGcpTempLocation(String value);

  /** Returns {@link PipelineOptions#getTempLocation} as the default GCP temp location. */
  class GcpTempLocationFactory implements DefaultValueFactory<String> {
    private static final FluentBackoff BACKOFF_FACTORY =
        FluentBackoff.DEFAULT.withMaxRetries(3).withInitialBackoff(Duration.millis(200));
    static final String DEFAULT_REGION = "us-central1";
    private static final Logger LOG = LoggerFactory.getLogger(GcpTempLocationFactory.class);

    @Override
    public @Nullable String create(PipelineOptions options) {
      String tempLocation = options.getTempLocation();
      if (isNullOrEmpty(tempLocation)) {
        tempLocation =
            tryCreateDefaultBucket(
                options,
                newCloudResourceManagerClient(options.as(CloudResourceManagerOptions.class))
                    .build());
        options.setTempLocation(tempLocation);
      } else {
        try {
          PathValidator validator = options.as(GcsOptions.class).getPathValidator();
          validator.validateOutputFilePrefixSupported(tempLocation);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format(
                  "Error constructing default value for gcpTempLocation: tempLocation is not"
                      + " a valid GCS path, %s. ",
                  tempLocation),
              e);
        }
      }

      if (isSoftDeletePolicyEnabled(options, tempLocation)) {
        LOG.warn(
            String.format(
                "The bucket of gcpTempLocation %s has soft delete policy enabled."
                    + " Dataflow jobs use Cloud Storage to store temporary files during pipeline"
                    + " execution. To avoid being billed for unnecessary storage costs, turn off the soft"
                    + " delete feature on buckets that your Dataflow jobs use for temporary storage."
                    + " For more information, see"
                    + " https://cloud.google.com/storage/docs/use-soft-delete#remove-soft-delete-policy.",
                tempLocation));
      }

      return tempLocation;
    }

    @VisibleForTesting
    static boolean isSoftDeletePolicyEnabled(PipelineOptions options, String tempLocation) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      GcsUtil gcsUtil = gcsOptions.getGcsUtil();
      try {
        SoftDeletePolicy policy =
            Objects.requireNonNull(gcsUtil.getBucket(GcsPath.fromUri(tempLocation)))
                .getSoftDeletePolicy();
        if (policy != null && policy.getRetentionDurationSeconds() > 0) {
          return true;
        }
      } catch (Exception e) {
        LOG.warn(
            String.format(
                "Failed to access bucket for gcpTempLocation: %s.%nCaused by %s", tempLocation, e));
      }
      return false;
    }

    @VisibleForTesting
    static ImmutableList<String> getDefaultBucketNameStubs(
        PipelineOptions options, CloudResourceManager crmClient, String bucketNamePrefix) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);

      final String projectId = gcsOptions.getProject();
      checkArgument(!isNullOrEmpty(projectId), "--project is a required option.");

      long projectNumber = 0L;
      try {
        projectNumber = getProjectNumber(projectId, crmClient);
      } catch (IOException e) {
        throw new RuntimeException("Unable to verify project with ID " + projectId, e);
      }

      String region = DEFAULT_REGION;
      if (!isNullOrEmpty(gcsOptions.getZone())) {
        region = getRegionFromZone(gcsOptions.getZone());
      }

      return ImmutableList.of(bucketNamePrefix, region, String.valueOf(projectNumber));
    }

    /**
     * Creates a default bucket or verifies the existence and proper access control of an existing
     * default bucket. Returns the location if successful.
     */
    @VisibleForTesting
    static String tryCreateDefaultBucket(PipelineOptions options, CloudResourceManager crmClient) {
      return tryCreateDefaultBucketWithPrefix(options, crmClient, "dataflow-staging");
    }

    @VisibleForTesting
    static String tryCreateDefaultBucketWithPrefix(
        PipelineOptions options, CloudResourceManager crmClient, String bucketNamePrefix) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);

      checkArgument(
          isNullOrEmpty(gcsOptions.getDataflowKmsKey()),
          "Cannot create a default bucket when --dataflowKmsKey is set.");

      final List<String> bucketNameStubs =
          getDefaultBucketNameStubs(options, crmClient, bucketNamePrefix);
      final String region = bucketNameStubs.get(1);
      final long projectNumber = Long.parseLong(bucketNameStubs.get(2));
      final String bucketName = String.join("-", bucketNameStubs);
      LOG.info("No tempLocation specified, attempting to use default bucket: {}", bucketName);

      // Disable soft delete policy for a bucket.
      // Reference: https://cloud.google.com/storage/docs/soft-delete
      SoftDeletePolicy softDeletePolicy = new SoftDeletePolicy().setRetentionDurationSeconds(0L);

      Bucket bucket =
          new Bucket()
              .setName(bucketName)
              .setLocation(region)
              .setSoftDeletePolicy(softDeletePolicy);
      // Always try to create the bucket before checking access, so that we do not
      // race with other pipelines that may be attempting to do the same thing.
      try {
        gcsOptions.getGcsUtil().createBucket(gcsOptions.getProject(), bucket);
      } catch (FileAlreadyExistsException e) {
        LOG.debug("Bucket '{}'' already exists, verifying access.", bucketName);
      } catch (IOException e) {
        throw new RuntimeException("Unable create default bucket.", e);
      }

      // Once the bucket is expected to exist, verify that it is correctly owned
      // by the project executing the job.
      try {
        long owner = gcsOptions.getGcsUtil().bucketOwner(GcsPath.fromComponents(bucketName, ""));
        checkArgument(
            owner == projectNumber,
            "Bucket owner does not match the project from --project:" + " %s vs. %s",
            owner,
            projectNumber);
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to determine the owner of the default bucket at gs://" + bucketName, e);
      }
      return "gs://" + bucketName + "/temp/";
    }

    /**
     * Returns the project number or throws an exception if the project does not exist or has other
     * access exceptions.
     */
    private static long getProjectNumber(String projectId, CloudResourceManager crmClient)
        throws IOException {
      return getProjectNumber(
          projectId,
          crmClient,
          BackOffAdapter.toGcpBackOff(BACKOFF_FACTORY.backoff()),
          Sleeper.DEFAULT);
    }

    /**
     * Returns the project number or throws an error if the project does not exist or has other
     * access errors.
     */
    private static long getProjectNumber(
        String projectId, CloudResourceManager crmClient, BackOff backoff, Sleeper sleeper)
        throws IOException {
      CloudResourceManager.Projects.Get getProject = crmClient.projects().get(projectId);
      try {
        Project project =
            ResilientOperation.retry(
                getProject::execute,
                backoff,
                RetryDeterminer.SOCKET_ERRORS,
                IOException.class,
                sleeper);
        return project.getProjectNumber();
      } catch (Exception e) {
        throw new IOException("Unable to get project number", e);
      }
    }

    @VisibleForTesting
    static String getRegionFromZone(String zone) {
      String[] zoneParts = zone.split("-", -1);
      checkArgument(zoneParts.length >= 2, "Invalid zone provided: %s", zone);
      return zoneParts[0] + "-" + zoneParts[1];
    }

    /**
     * Returns a CloudResourceManager client builder using the specified {@link
     * CloudResourceManagerOptions}.
     */
    @VisibleForTesting
    static CloudResourceManager.Builder newCloudResourceManagerClient(
        CloudResourceManagerOptions options) {
      Credentials credentials = options.getGcpCredential();
      if (credentials == null) {
        NullCredentialInitializer.throwNullCredentialException();
      }
      return new CloudResourceManager.Builder(
              Transport.getTransport(),
              Transport.getJsonFactory(),
              chainHttpRequestInitializer(
                  credentials,
                  // Do not log 404. It clutters the output and is possibly even required by the
                  // caller.
                  new RetryHttpRequestInitializer(ImmutableList.of(404))))
          .setApplicationName(options.getAppName())
          .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
    }

    private static HttpRequestInitializer chainHttpRequestInitializer(
        Credentials credential, HttpRequestInitializer httpRequestInitializer) {
      if (credential == null) {
        return new ChainingHttpRequestInitializer(
            new NullCredentialInitializer(), httpRequestInitializer);
      } else {
        return new ChainingHttpRequestInitializer(
            new HttpCredentialsAdapter(credential), httpRequestInitializer);
      }
    }
  }

  /**
   * GCP <a href="https://cloud.google.com/kms/">Cloud KMS</a> key for Dataflow pipelines and
   * buckets created by GcpTempLocationFactory.
   */
  @Description(
      "GCP Cloud KMS key for Dataflow pipelines. Also used by gcpTempLocation as the default key "
          + "for new buckets. Key format is: "
          + "projects/<project>/locations/<location>/keyRings/<keyring>/cryptoKeys/<key>")
  @Nullable
  String getDataflowKmsKey();

  void setDataflowKmsKey(String dataflowKmsKey);
}
