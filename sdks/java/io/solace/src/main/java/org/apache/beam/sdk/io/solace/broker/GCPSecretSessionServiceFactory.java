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
package org.apache.beam.sdk.io.solace.broker;

import static org.apache.beam.sdk.io.solace.broker.SessionService.DEFAULT_VPN_NAME;

import com.google.auto.value.AutoValue;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a {@link SessionServiceFactory} that retrieve the basic authentication
 * credentials from a Google Cloud Secret Manager secret.
 *
 * <p>It can be used to avoid having to pass the password as an option of your pipeline. For this
 * provider to work, the worker where the job runs needs to have the necessary credentials to access
 * the secret. In Dataflow, this implies adding the necessary permissions to the worker service
 * account. For other runners, set the credentials in the pipeline options using {@link
 * org.apache.beam.sdk.extensions.gcp.options.GcpOptions}.
 *
 * <p>It also shows how to implement a {@link SessionServiceFactory} that depends on using external
 * resources to retrieve the Solace session properties. In this case, using the Google Cloud Secrete
 * Manager client.
 *
 * <p>Example of how to create the provider object:
 *
 * <pre>{@code
 * GCPSecretSessionServiceFactory factory =
 *     GCPSecretSessionServiceFactory.builder()
 *         .username("user")
 *         .host("host:port")
 *         .passwordSecretName("secret-name")
 *         .build();
 *
 * SessionService serviceUsingGCPSecret = factory.create();
 * }</pre>
 */
@AutoValue
public abstract class GCPSecretSessionServiceFactory extends SessionServiceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GCPSecretSessionServiceFactory.class);

  private static final String PROJECT_NOT_FOUND = "PROJECT-NOT-FOUND";

  public abstract String username();

  public abstract String host();

  public abstract String passwordSecretName();

  public abstract String vpnName();

  public abstract @Nullable String secretManagerProjectId();

  public abstract String passwordSecretVersion();

  public static GCPSecretSessionServiceFactory.Builder builder() {
    return new AutoValue_GCPSecretSessionServiceFactory.Builder()
        .passwordSecretVersion("latest")
        .vpnName(DEFAULT_VPN_NAME);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /** Username to be used to authenticate with the broker. */
    public abstract GCPSecretSessionServiceFactory.Builder username(String username);

    /**
     * The location of the broker, including port details if it is not listening in the default
     * port.
     */
    public abstract GCPSecretSessionServiceFactory.Builder host(String host);

    /** The Secret Manager secret name where the password is stored. */
    public abstract GCPSecretSessionServiceFactory.Builder passwordSecretName(String name);

    /** Optional. Solace broker VPN name. If not set, "default" is used. */
    public abstract GCPSecretSessionServiceFactory.Builder vpnName(String name);

    /**
     * Optional for Dataflow or VMs running on Google Cloud. The project id of the project where the
     * secret is stored. If not set, the project id where the job is running is used.
     */
    public abstract GCPSecretSessionServiceFactory.Builder secretManagerProjectId(String id);

    /** Optional. Solace broker password secret version. If not set, "latest" is used. */
    public abstract GCPSecretSessionServiceFactory.Builder passwordSecretVersion(String version);

    public abstract GCPSecretSessionServiceFactory build();
  }

  @Override
  public SessionService create() {
    String password = null;
    try {
      password = retrieveSecret();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    BasicAuthJcsmpSessionServiceFactory factory =
        BasicAuthJcsmpSessionServiceFactory.builder()
            .username(username())
            .host(host())
            .password(password)
            .vpnName(vpnName())
            .build();

    return factory.create();
  }

  private String retrieveSecret() throws IOException {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      String projectId =
          Optional.ofNullable(secretManagerProjectId()).orElse(getProjectIdFromVmMetadata());
      SecretVersionName secretVersionName =
          SecretVersionName.of(projectId, passwordSecretName(), passwordSecretVersion());
      return client.accessSecretVersion(secretVersionName).getPayload().getData().toStringUtf8();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getProjectIdFromVmMetadata() throws IOException {
    URL metadataUrl =
        new URL("http://metadata.google.internal/computeMetadata/v1/project/project-id");
    HttpURLConnection connection = (HttpURLConnection) metadataUrl.openConnection();
    connection.setRequestProperty("Metadata-Flavor", "Google");

    String output;
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
      output = reader.readLine();
    }

    if (output == null || output.isEmpty()) {
      LOG.error(
          "Cannot retrieve project id from VM metadata, please set a project id in your GoogleCloudSecretProvider.");
    }
    return output != null ? output : PROJECT_NOT_FOUND;
  }
}
