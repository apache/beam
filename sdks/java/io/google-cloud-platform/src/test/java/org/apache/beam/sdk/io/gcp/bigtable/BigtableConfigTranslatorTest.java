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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

/** Unit tests for {@link BigtableConfigTranslator}. */
@RunWith(JUnit4.class)
public class BigtableConfigTranslatorTest {

  @Test
  public void testBigtableOptionsToBigtableConfig() throws Exception {
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId("project")
            .setInstanceId("instance")
            .setAppProfileId("app-profile")
            .setDataHost("localhost")
            .setPort(1234)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .build();

    BigtableConfig config = BigtableConfig.builder().setValidate(true).build();
    config =
        BigtableConfigTranslator.translateToBigtableConfig(
            config, options, PipelineOptionsFactory.as(GcpOptions.class));

    assertNotNull(config.getProjectId());
    assertNotNull(config.getInstanceId());
    assertNotNull(config.getAppProfileId());
    assertNotNull(config.getEmulatorHost());
    assertNotNull(config.getCredentialFactory());

    NoopCredentialFactory noopCredentialFactory =
        NoopCredentialFactory.fromOptions(PipelineOptionsFactory.as(GcpOptions.class));
    assertEquals(options.getProjectId(), config.getProjectId().get());
    assertEquals(options.getInstanceId(), config.getInstanceId().get());
    assertEquals(options.getAppProfileId(), config.getAppProfileId().get());
    assertEquals("localhost:1234", config.getEmulatorHost());
    assertEquals(
        noopCredentialFactory.getCredential(), config.getCredentialFactory().getCredential());
  }

  @Test
  public void testBigtableOptionsToBigtableReadOptions() {
    BigtableOptions options =
        BigtableOptions.builder()
            .setCallOptionsConfig(
                CallOptionsConfig.builder()
                    .setReadRowsRpcAttemptTimeoutMs(100)
                    .setReadRowsRpcTimeoutMs(1000)
                    .build())
            .setRetryOptions(
                RetryOptions.builder().setInitialBackoffMillis(5).setBackoffMultiplier(1.5).build())
            .build();

    BigtableReadOptions readOptions =
        BigtableReadOptions.builder()
            .setTableId(ValueProvider.StaticValueProvider.of("table"))
            .build();

    readOptions = BigtableConfigTranslator.translateToBigtableReadOptions(readOptions, options);

    assertNotNull(readOptions.getAttemptTimeout());
    assertNotNull(readOptions.getOperationTimeout());
    assertNotNull(readOptions.getRetryInitialDelay());
    assertNotNull(readOptions.getRetryDelayMultiplier());

    assertEquals(100, (long) readOptions.getAttemptTimeout());
    assertEquals(1000, (long) readOptions.getOperationTimeout());
    assertEquals(5, (long) readOptions.getRetryInitialDelay());
    assertEquals(1.5, (double) readOptions.getRetryDelayMultiplier(), 0);
  }

  @Test
  public void testBigtableOptionsToBigtableWriteOptions() {
    BigtableOptions options =
        BigtableOptions.builder()
            .setCallOptionsConfig(
                CallOptionsConfig.builder()
                    .setMutateRpcAttemptTimeoutMs(200)
                    .setMutateRpcTimeoutMs(2000)
                    .build())
            .setRetryOptions(
                RetryOptions.builder()
                    .setInitialBackoffMillis(15)
                    .setBackoffMultiplier(2.5)
                    .build())
            .setBulkOptions(
                BulkOptions.builder()
                    .setBulkMaxRequestSize(20)
                    .setBulkMaxRowKeyCount(100)
                    .setMaxInflightRpcs(5)
                    .build())
            .build();

    BigtableWriteOptions writeOptions =
        BigtableWriteOptions.builder()
            .setTableId(ValueProvider.StaticValueProvider.of("table"))
            .build();

    writeOptions = BigtableConfigTranslator.translateToBigtableWriteOptions(writeOptions, options);

    assertNotNull(writeOptions.getAttemptTimeout());
    assertNotNull(writeOptions.getOperationTimeout());
    assertNotNull(writeOptions.getRetryInitialDelay());
    assertNotNull(writeOptions.getRetryDelayMultiplier());
    assertNotNull(writeOptions.getBatchBytes());
    assertNotNull(writeOptions.getBatchElements());
    assertNotNull(writeOptions.getMaxRequests());

    assertEquals(200, (long) writeOptions.getAttemptTimeout());
    assertEquals(2000, (long) writeOptions.getOperationTimeout());
    assertEquals(15, (long) writeOptions.getRetryInitialDelay());
    assertEquals(2.5, writeOptions.getRetryDelayMultiplier(), 0);
    assertEquals(20, (long) writeOptions.getBatchBytes());
    assertEquals(100, (long) writeOptions.getBatchElements());
    assertEquals(5, (long) writeOptions.getMaxRequests());
  }

  @Test
  public void testVeneerReadSettings() {
    BigtableConfig config =
        BigtableConfig.builder()
            .setProjectId(ValueProvider.StaticValueProvider.of("project"))
            .setInstanceId(ValueProvider.StaticValueProvider.of("instance"))
            .setAppProfileId(ValueProvider.StaticValueProvider.of("app"))
            .setValidate(true)
            .build();
    BigtableReadOptions readOptions =
        BigtableReadOptions.builder()
            .setTableId(ValueProvider.StaticValueProvider.of("table"))
            .setAttemptTimeout(101)
            .setOperationTimeout(1001)
            .setRetryInitialDelay(5)
            .setRetryDelayMultiplier(1.5)
            .build();
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    BigtableDataSettings settings =
        BigtableConfigTranslator.translateReadToVeneerSettings(
            config, readOptions, pipelineOptions);

    EnhancedBigtableStubSettings stubSettings = settings.getStubSettings();

    assertEquals(config.getProjectId().get(), stubSettings.getProjectId());
    assertEquals(config.getInstanceId().get(), stubSettings.getInstanceId());
    assertEquals(config.getAppProfileId().get(), stubSettings.getAppProfileId());
    assertEquals(
        Duration.ofMillis(101),
        stubSettings.readRowsSettings().getRetrySettings().getInitialRpcTimeout());
    assertEquals(
        Duration.ofMillis(1001),
        stubSettings.readRowsSettings().getRetrySettings().getTotalTimeout());
    assertEquals(
        Duration.ofMillis(5),
        stubSettings.readRowsSettings().getRetrySettings().getInitialRetryDelay());
    assertEquals(
        1.5, stubSettings.readRowsSettings().getRetrySettings().getRetryDelayMultiplier(), 0);
  }

  @Test
  public void testVeneerWriteSettings() {
    BigtableConfig config =
        BigtableConfig.builder()
            .setProjectId(ValueProvider.StaticValueProvider.of("project"))
            .setInstanceId(ValueProvider.StaticValueProvider.of("instance"))
            .setAppProfileId(ValueProvider.StaticValueProvider.of("app"))
            .setValidate(true)
            .build();
    BigtableWriteOptions writeOptions =
        BigtableWriteOptions.builder()
            .setTableId(ValueProvider.StaticValueProvider.of("table"))
            .setAttemptTimeout(101)
            .setOperationTimeout(1001)
            .setRetryInitialDelay(5)
            .setRetryDelayMultiplier(1.5)
            .setMaxRequests(11)
            .setBatchElements(105)
            .setBatchBytes(102)
            .build();
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    BigtableDataSettings settings =
        BigtableConfigTranslator.translateWriteToVeneerSettings(
            config, writeOptions, pipelineOptions);

    EnhancedBigtableStubSettings stubSettings = settings.getStubSettings();

    assertEquals(config.getProjectId().get(), stubSettings.getProjectId());
    assertEquals(config.getInstanceId().get(), stubSettings.getInstanceId());
    assertEquals(config.getAppProfileId().get(), stubSettings.getAppProfileId());
    assertEquals(
        Duration.ofMillis(101),
        stubSettings.bulkMutateRowsSettings().getRetrySettings().getInitialRpcTimeout());
    assertEquals(
        Duration.ofMillis(1001),
        stubSettings.bulkMutateRowsSettings().getRetrySettings().getTotalTimeout());
    assertEquals(
        Duration.ofMillis(5),
        stubSettings.bulkMutateRowsSettings().getRetrySettings().getInitialRetryDelay());
    assertEquals(
        1.5, stubSettings.bulkMutateRowsSettings().getRetrySettings().getRetryDelayMultiplier(), 0);
    assertEquals(
        105,
        (long)
            stubSettings.bulkMutateRowsSettings().getBatchingSettings().getElementCountThreshold());
    assertEquals(
        102,
        (long)
            stubSettings.bulkMutateRowsSettings().getBatchingSettings().getRequestByteThreshold());
    assertEquals(
        105 * 11,
        (long)
            stubSettings
                .bulkMutateRowsSettings()
                .getBatchingSettings()
                .getFlowControlSettings()
                .getMaxOutstandingElementCount());

    assertEquals(
        102 * 11,
        (long)
            stubSettings
                .bulkMutateRowsSettings()
                .getBatchingSettings()
                .getFlowControlSettings()
                .getMaxOutstandingRequestBytes());
  }
}
