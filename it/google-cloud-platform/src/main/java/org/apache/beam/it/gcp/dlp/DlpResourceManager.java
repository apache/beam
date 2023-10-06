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
package org.apache.beam.it.gcp.dlp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.privacy.dlp.v2.DeidentifyTemplate;
import com.google.privacy.dlp.v2.ProjectName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for managing Google Cloud DLP (Data Loss Prevention) resources. */
public class DlpResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DlpResourceManager.class);

  private final String project;
  private final CredentialsProvider credentialsProvider;

  /**
   * Constructs a new DlpResourceManager with the specified project and credentials provider.
   *
   * @param project the GCP project ID
   * @param credentialsProvider the credentials provider for authentication
   */
  public DlpResourceManager(String project, CredentialsProvider credentialsProvider) {
    this.project = project;
    this.credentialsProvider = credentialsProvider;
  }

  private final List<String> createdTemplates = new ArrayList<>();

  /**
   * Retrieves a DlpServiceClient with the configured settings.
   *
   * @return a DlpServiceClient instance
   * @throws IOException if an error occurs during client creation
   */
  public DlpServiceClient getDlpClient() throws IOException {
    DlpServiceSettings.Builder dlpBuilder = DlpServiceSettings.newBuilder();
    if (credentialsProvider != null) {
      dlpBuilder = dlpBuilder.setCredentialsProvider(credentialsProvider);
    }
    return DlpServiceClient.create(dlpBuilder.build());
  }

  /**
   * Creates a deidentify template in the specified project.
   *
   * @param template the deidentify template to create
   * @return the created DeidentifyTemplate
   * @throws IOException if an error occurs during template creation
   */
  public DeidentifyTemplate createDeidentifyTemplate(DeidentifyTemplate template)
      throws IOException {
    try (DlpServiceClient client = getDlpClient()) {
      DeidentifyTemplate deidentifyTemplate =
          client.createDeidentifyTemplate(ProjectName.of(this.project), template);

      createdTemplates.add(deidentifyTemplate.getName());

      return deidentifyTemplate;
    }
  }

  /**
   * Removes a deidentify template by its name.
   *
   * @param templateName the name of the template to remove
   * @throws IOException if an error occurs during template deletion
   */
  public void removeDeidentifyTemplate(String templateName) throws IOException {
    try (DlpServiceClient client = getDlpClient()) {
      client.deleteDeidentifyTemplate(templateName);
    }
  }

  @Override
  public void cleanupAll() {
    for (String templateName : createdTemplates) {
      try {
        removeDeidentifyTemplate(templateName);
      } catch (Exception e) {
        LOG.error("Error deleting managed template: {}", templateName, e);
      }
    }
    createdTemplates.clear();
  }

  /**
   * Creates a new Builder for constructing a DlpResourceManager instance.
   *
   * @param project the GCP project ID
   * @return a new instance of Builder
   */
  public static DlpResourceManager.Builder builder(
      String project, CredentialsProvider credentialsProvider) {
    return new DlpResourceManager.Builder(project, credentialsProvider);
  }

  /** A builder class for creating instances of {@link DlpResourceManager}. */
  public static class Builder {

    private final String project;
    private CredentialsProvider credentialsProvider;

    /**
     * Constructs a new Builder with the specified project.
     *
     * @param project the GCP project ID
     */
    public Builder(String project, CredentialsProvider credentialsProvider) {
      this.project = project;
      this.credentialsProvider = credentialsProvider;
    }

    /**
     * Set the credentials provider to use with this client.
     *
     * @param credentialsProvider the CredentialsProvider instance
     * @return the Builder instance
     */
    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /**
     * Builds a new instance of {@link DlpResourceManager} with the specified project.
     *
     * @return a new instance of {@link DlpResourceManager}
     * @throws IllegalArgumentException if the project is not set
     */
    public DlpResourceManager build() {
      if (project == null) {
        throw new IllegalArgumentException(
            "A GCP project must be provided to build a DLP resource manager.");
      }
      return new DlpResourceManager(project, credentialsProvider);
    }
  }
}
