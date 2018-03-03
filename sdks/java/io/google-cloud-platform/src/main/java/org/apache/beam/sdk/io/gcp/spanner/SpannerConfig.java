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
package org.apache.beam.sdk.io.gcp.spanner;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.ReleaseInfo;

/** Configuration for a Cloud Spanner client. */
@AutoValue
public abstract class SpannerConfig implements Serializable {
  // A common user agent token that indicates that this request was originated from Apache Beam.
  private static final String USER_AGENT_PREFIX = "Apache_Beam_Java";
  // A default host name for batch traffic.
  private static final String DEFAULT_HOST = "https://batch-spanner.googleapis.com/";

  @Nullable
  abstract ValueProvider<String> getProjectId();

  @Nullable
  abstract ValueProvider<String> getInstanceId();

  @Nullable
  abstract ValueProvider<String> getDatabaseId();

  @Nullable
  abstract String getHost();

  @Nullable
  @VisibleForTesting
  abstract ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

  abstract Builder toBuilder();

  public static SpannerConfig create() {
    return builder().setHost(DEFAULT_HOST).build();
  }

  static Builder builder() {
    return new AutoValue_SpannerConfig.Builder();
  }

  public void validate() {
    checkNotNull(
        getInstanceId(),
        "SpannerIO.read() requires instance id to be set with withInstanceId method");
    checkNotNull(
        getDatabaseId(),
        "SpannerIO.read() requires database id to be set with withDatabaseId method");
  }

  public void populateDisplayData(DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("Output Project"))
        .addIfNotNull(DisplayData.item("instanceId", getInstanceId()).withLabel("Output Instance"))
        .addIfNotNull(DisplayData.item("databaseId", getDatabaseId()).withLabel("Output Database"));

    if (getServiceFactory() != null) {
      builder.addIfNotNull(
          DisplayData.item("serviceFactory", getServiceFactory().getClass().getName())
              .withLabel("Service Factory"));
    }
  }

  /** Builder for {@link SpannerConfig}. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setProjectId(ValueProvider<String> projectId);

    abstract Builder setInstanceId(ValueProvider<String> instanceId);

    abstract Builder setDatabaseId(ValueProvider<String> databaseId);

    abstract Builder setHost(String host);

    abstract Builder setServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory);

    public abstract SpannerConfig build();
  }

  public SpannerConfig withProjectId(ValueProvider<String> projectId) {
    return toBuilder().setProjectId(projectId).build();
  }

  public SpannerConfig withProjectId(String projectId) {
    return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
  }

  public SpannerConfig withInstanceId(ValueProvider<String> instanceId) {
    return toBuilder().setInstanceId(instanceId).build();
  }

  public SpannerConfig withInstanceId(String instanceId) {
    return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
  }

  public SpannerConfig withDatabaseId(ValueProvider<String> databaseId) {
    return toBuilder().setDatabaseId(databaseId).build();
  }

  public SpannerConfig withDatabaseId(String databaseId) {
    return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
  }

  public SpannerConfig withHost(String host) {
    return toBuilder().setHost(host).build();
  }

  @VisibleForTesting
  SpannerConfig withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
    return toBuilder().setServiceFactory(serviceFactory).build();
  }

  public SpannerAccessor connectToSpanner() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();
    if (getProjectId() != null) {
      builder.setProjectId(getProjectId().get());
    }
    if (getServiceFactory() != null) {
      builder.setServiceFactory(this.getServiceFactory());
    }
    if (getHost() != null) {
      builder.setHost(getHost());
    }
    ReleaseInfo releaseInfo = ReleaseInfo.getReleaseInfo();
    builder.setUserAgentPrefix(USER_AGENT_PREFIX + "/" + releaseInfo.getVersion());
    SpannerOptions options = builder.build();
    Spanner spanner = options.getService();
    DatabaseClient databaseClient = spanner.getDatabaseClient(
        DatabaseId.of(options.getProjectId(), getInstanceId().get(), getDatabaseId().get()));
    BatchClient batchClient = spanner.getBatchClient(
        DatabaseId.of(options.getProjectId(), getInstanceId().get(), getDatabaseId().get()));
    return new SpannerAccessor(spanner, databaseClient, batchClient);
  }

}
