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

package org.apache.beam.sdk.io.gcp.bigtable;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;

/**
 * Configuration for a Cloud Bigtable client.
 */
@AutoValue
abstract class BigtableConfig implements Serializable {

  /**
   * Returns the project id being written to.
   */
  @Nullable
  abstract ValueProvider<String> getProjectId();

  /**
   * Returns the instance id being written to.
   */
  @Nullable
  abstract ValueProvider<String> getInstanceId();

  /**
   * Returns the table being read from.
   */
  @Nullable
  abstract ValueProvider<String> getTableId();

  /**
   * Configurator of the effective Bigtable Options.
   */
  @Nullable
  abstract SerializableFunction<BigtableOptions.Builder,
    BigtableOptions.Builder> getBigtableOptionsConfigurator();

  /**
   * Weather validate that table exists before writing.
   */
  abstract boolean getValidate();

  /**
   * {@link BigtableService} used only for testing.
   */
  @Nullable
  abstract BigtableService getBigtableService();

  abstract Builder toBuilder();

  static BigtableConfig.Builder builder() {
    return new AutoValue_BigtableConfig.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setProjectId(ValueProvider<String> projectId);

    abstract Builder setInstanceId(ValueProvider<String> instanceId);

    abstract Builder setTableId(ValueProvider<String> tableId);

    abstract Builder setValidate(boolean validate);

    abstract Builder setBigtableOptionsConfigurator(
      SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> optionsConfigurator);

    abstract Builder setBigtableService(BigtableService bigtableService);

    abstract BigtableConfig build();
  }

  BigtableConfig withProjectId(ValueProvider<String> projectId) {
    checkNotNull(projectId, "Project Id of BigTable can not be null");
    return toBuilder().setProjectId(projectId).build();
  }

  BigtableConfig withInstanceId(ValueProvider<String> instanceId) {
    checkNotNull(instanceId, "Instance Id of BigTable can not be null");
    return toBuilder().setInstanceId(instanceId).build();
  }

  BigtableConfig withTableId(ValueProvider<String> tableId) {
    checkNotNull(tableId, "tableId can not be null");
    return toBuilder().setTableId(tableId).build();
  }

  BigtableConfig withBigtableOptionsConfigurator(
    SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> configurator) {
    checkNotNull(configurator, "configurator can not be null");
    return toBuilder().setBigtableOptionsConfigurator(configurator).build();
  }

  BigtableConfig withValidate(boolean isEnabled) {
    return toBuilder().setValidate(isEnabled).build();
  }

  @VisibleForTesting
  BigtableConfig withBigtableService(BigtableService bigtableService) {
    checkNotNull(bigtableService, "bigtableService can not be null");
    return toBuilder().setBigtableService(bigtableService).build();
  }

  void validate() {
    checkNotNull(getProjectId(), "Could not obtain Bigtable project id");
    checkNotNull(getInstanceId(), "Could not obtain Bigtable instance id");
    checkNotNull(getTableId(), "Could not obtain Bigtable table id");
  }

  void populateDisplayData(DisplayData.Builder builder) {
    builder
      .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("Bigtable Project Id"))
      .addIfNotNull(DisplayData.item("instanceId", getInstanceId())
        .withLabel("Bigtable Instance Id"))
      .addIfNotNull(DisplayData.item("tableId", getTableId()).withLabel("Bigtable Table ID"))
      .add(DisplayData.item("withValidation", getValidate()).withLabel("Check is table exists"));

    builder.add(DisplayData.item("effectiveBigtableOptions",
      effectiveUserProvidedBigtableOptions().build().toString())
      .withLabel("Effective BigtableOptions resulted from configuration of given options"));
  }

  /**
   * Helper function that either returns the mock Bigtable service supplied by
   * {@link #withBigtableService} or creates and returns an implementation that talks to
   * {@code Cloud Bigtable}.
   *
   * <p>Also populate the credentials option from {@link GcpOptions#getGcpCredential()} if the
   * default credentials are being used on {@link BigtableOptions}.
   */
  @VisibleForTesting
  BigtableService getBigtableService(PipelineOptions pipelineOptions) {
    if (getBigtableService() != null) {
      return getBigtableService();
    }

    BigtableOptions.Builder bigtableOptions = effectiveUserProvidedBigtableOptions();

    bigtableOptions.setUserAgent(pipelineOptions.getUserAgent());

    if (bigtableOptions.build().getCredentialOptions().getCredentialType()
      == CredentialOptions.CredentialType.DefaultCredentials) {
      bigtableOptions.setCredentialOptions(
        CredentialOptions.credential(
          pipelineOptions.as(GcpOptions.class).getGcpCredential()));
    }

    // Default option that should be forced
    bigtableOptions.setUseCachedDataPool(true);

    return new BigtableServiceImpl(bigtableOptions.build());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(BigtableConfig.class)
      .add("projectId", getProjectId())
      .add("instanceId", getInstanceId())
      .add("tableId", getTableId())
      .add("bigtableOptionsConfigurator",
        getBigtableOptionsConfigurator() == null ? null : getBigtableOptionsConfigurator()
          .getClass().getName())
      .add("effectiveOptions", effectiveUserProvidedBigtableOptions())
      .toString();
  }

  private BigtableOptions.Builder effectiveUserProvidedBigtableOptions() {
    BigtableOptions.Builder effectiveOptions = new BigtableOptions.Builder();

    if (getBigtableOptionsConfigurator() != null) {
      effectiveOptions = getBigtableOptionsConfigurator().apply(effectiveOptions);
    }

    return effectiveOptions
      .setInstanceId(getInstanceId().get())
      .setProjectId(getProjectId().get());
  }
}
