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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.beam.sdk.options.CloudResourceManagerOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides operations on Google Cloud Platform Projects.
 */
public class GcpProjectUtil {
  /**
   * A {@link DefaultValueFactory} able to create a {@link GcpProjectUtil} using
   * any transport flags specified on the {@link PipelineOptions}.
   */
  public static class GcpProjectUtilFactory implements DefaultValueFactory<GcpProjectUtil> {
    /**
     * Returns an instance of {@link GcpProjectUtil} based on the
     * {@link PipelineOptions}.
     */
    @Override
    public GcpProjectUtil create(PipelineOptions options) {
      LOG.debug("Creating new GcpProjectUtil");
      CloudResourceManagerOptions crmOptions = options.as(CloudResourceManagerOptions.class);
      return new GcpProjectUtil(
          Transport.newCloudResourceManagerClient(crmOptions).build());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(GcpProjectUtil.class);

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(3).withInitialBackoff(Duration.millis(200));

  /** Client for the CRM API. */
  private CloudResourceManager crmClient;

  private GcpProjectUtil(CloudResourceManager crmClient) {
    this.crmClient = crmClient;
  }

  // Use this only for testing purposes.
  @VisibleForTesting
  void setCrmClient(CloudResourceManager crmClient) {
    this.crmClient = crmClient;
  }

  /**
   * Returns the project number or throws an exception if the project does not
   * exist or has other access exceptions.
   */
  public long getProjectNumber(String projectId) throws IOException {
    return getProjectNumber(
      projectId,
      BACKOFF_FACTORY.backoff(),
      Sleeper.DEFAULT);
  }

  /**
   * Returns the project number or throws an error if the project does not
   * exist or has other access errors.
   */
  @VisibleForTesting
  long getProjectNumber(String projectId, BackOff backoff, Sleeper sleeper) throws IOException {
      CloudResourceManager.Projects.Get getProject =
          crmClient.projects().get(projectId);
      try {
        Project project = ResilientOperation.retry(
            ResilientOperation.getGoogleRequestCallable(getProject),
            backoff,
            RetryDeterminer.SOCKET_ERRORS,
            IOException.class,
            sleeper);
        return project.getProjectNumber();
      } catch (Exception e) {
        throw new IOException("Unable to get project number", e);
     }
  }
}
