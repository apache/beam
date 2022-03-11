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
package org.apache.beam.sdk.io.cdap.github.batch;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.cdap.github.common.SchemaBuilder;
import org.apache.beam.sdk.io.cdap.github.common.model.GitHubModel;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Branch;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Collaborator;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Comment;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Commit;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Content;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.DeployKey;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Deployment;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Fork;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Invitation;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Page;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Release;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.TrafficReferrer;
import org.apache.beam.sdk.io.cdap.github.common.model.impl.Webhook;

/** Provides all required configuration for reading Github data from Batch Source. */
public class GithubBatchSourceConfig extends ReferencePluginConfig {

  public static final String AUTHORIZATION_TOKEN = "authorizationToken";
  public static final String AUTHORIZATION_TOKEN_DISPLAY_NAME = "Authorization token";
  public static final String REPOSITORY_OWNER = "repoOwner";
  public static final String REPOSITORY_OWNER_DISPLAY_NAME = "Repository owner name";
  public static final String REPOSITORY_NAME = "repoName";
  public static final String REPOSITORY_NAME_DISPLAY_NAME = "Repository name";
  public static final String DATASET_NAME = "datasetName";
  public static final String DATASET_NAME_DISPLAY_NAME = "Dataset name";
  public static final String HOSTNAME = "hostname";

  @Name(AUTHORIZATION_TOKEN)
  @Description("Authorization token to access GitHub API")
  @Macro
  protected String authorizationToken;

  @Name(REPOSITORY_OWNER)
  @Description("GitHub repository owner")
  @Macro
  protected String repoOwner;

  @Name(REPOSITORY_NAME)
  @Description("GitHub repository name")
  @Macro
  protected String repoName;

  @Name(DATASET_NAME)
  @Description("Dataset name that you would like to retrieve")
  @Macro
  protected String datasetName;

  @Name(HOSTNAME)
  @Description("GitHub API hostname")
  @Nullable
  @Macro
  protected String hostname;

  private transient Schema schema = null;

  public GithubBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  /**
   * Returns selected Schema.
   *
   * @return instance of Schema
   */
  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(datasetName, getDatasetClass());
    }
    return schema;
  }

  public String getAuthorizationToken() {
    return authorizationToken;
  }

  public String getRepoOwner() {
    return repoOwner;
  }

  public String getRepoName() {
    return repoName;
  }

  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Returns selected datasetClass.
   *
   * @return instance of datasetClass
   */
  public Class<? extends GitHubModel> getDatasetClass() {
    switch (datasetName) {
      case "Branches":
        {
          return Branch.class;
        }
      case "Collaborators":
        {
          return Collaborator.class;
        }
      case "Comments":
        {
          return Comment.class;
        }
      case "Commits":
        {
          return Commit.class;
        }
      case "Contents":
        {
          return Content.class;
        }
      case "Deploy Keys":
        {
          return DeployKey.class;
        }
      case "Deployments":
        {
          return Deployment.class;
        }
      case "Forks":
        {
          return Fork.class;
        }
      case "Invitations":
        {
          return Invitation.class;
        }
      case "Pages":
        {
          return Page.class;
        }
      case "Releases":
        {
          return Release.class;
        }
      case "Traffic:Referrers":
        {
          return TrafficReferrer.class;
        }
      case "Webhooks":
        {
          return Webhook.class;
        }
      default:
        {
          throw new IllegalArgumentException("Unsupported dataset name!");
        }
    }
  }

  @Nullable
  public String getHostname() {
    return hostname;
  }

  /** Validates {@link GithubBatchSourceConfig} instance. */
  public void validate(FailureCollector failureCollector) {
    if (!containsMacro(AUTHORIZATION_TOKEN) && Strings.isNullOrEmpty(authorizationToken)) {
      failureCollector
          .addFailure(
              String.format("%s must be specified.", AUTHORIZATION_TOKEN_DISPLAY_NAME), null)
          .withConfigProperty(AUTHORIZATION_TOKEN);
    }
    if (!containsMacro(REPOSITORY_OWNER) && Strings.isNullOrEmpty(repoOwner)) {
      failureCollector
          .addFailure(String.format("%s must be specified.", REPOSITORY_OWNER_DISPLAY_NAME), null)
          .withConfigProperty(REPOSITORY_OWNER);
    }
    if (!containsMacro(REPOSITORY_NAME) && Strings.isNullOrEmpty(repoName)) {
      failureCollector
          .addFailure(String.format("%s must be specified.", REPOSITORY_NAME_DISPLAY_NAME), null)
          .withConfigProperty(REPOSITORY_NAME);
    }
    if (!containsMacro(DATASET_NAME) && Strings.isNullOrEmpty(datasetName)) {
      failureCollector
          .addFailure(String.format("%s must be specified.", DATASET_NAME_DISPLAY_NAME), null)
          .withConfigProperty(DATASET_NAME);
    }
  }
}
