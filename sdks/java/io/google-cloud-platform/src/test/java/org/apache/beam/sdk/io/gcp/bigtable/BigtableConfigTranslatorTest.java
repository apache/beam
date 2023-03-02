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

import com.google.auth.Credentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
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
    config = BigtableConfigTranslator.translateToBigtableConfig(config, options);

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
  public void testBigtableOptionsToBigtableReadOptions() throws Exception {
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

    assertEquals(org.joda.time.Duration.millis(100), readOptions.getAttemptTimeout());
    assertEquals(org.joda.time.Duration.millis(1000), readOptions.getOperationTimeout());
  }

  @Test
  public void testBigtableOptionsToBigtableWriteOptions() throws Exception {
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
    assertNotNull(writeOptions.getMaxBytesPerBatch());
    assertNotNull(writeOptions.getMaxElementsPerBatch());
    assertNotNull(writeOptions.getMaxOutstandingElements());
    assertNotNull(writeOptions.getMaxOutstandingBytes());

    assertEquals(org.joda.time.Duration.millis(200), writeOptions.getAttemptTimeout());
    assertEquals(org.joda.time.Duration.millis(2000), writeOptions.getOperationTimeout());
    assertEquals(20, (long) writeOptions.getMaxBytesPerBatch());
    assertEquals(100, (long) writeOptions.getMaxElementsPerBatch());
    assertEquals(5 * 100, (long) writeOptions.getMaxOutstandingElements());
    assertEquals(5 * 20, (long) writeOptions.getMaxOutstandingBytes());
  }

  @Test
  public void testVeneerReadSettings() throws Exception {
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
            .setAttemptTimeout(org.joda.time.Duration.millis(101))
            .setOperationTimeout(org.joda.time.Duration.millis(1001))
            .build();

    PipelineOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);

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
  }

  @Test
  public void testVeneerWriteSettings() throws Exception {
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
            .setAttemptTimeout(org.joda.time.Duration.millis(101))
            .setOperationTimeout(org.joda.time.Duration.millis(1001))
            .setMaxElementsPerBatch(105)
            .setMaxBytesPerBatch(102)
            .setMaxOutstandingElements(10001)
            .setMaxOutstandingBytes(100001)
            .build();

    PipelineOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);

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
        105,
        (long)
            stubSettings.bulkMutateRowsSettings().getBatchingSettings().getElementCountThreshold());
    assertEquals(
        102,
        (long)
            stubSettings.bulkMutateRowsSettings().getBatchingSettings().getRequestByteThreshold());
    assertEquals(
        10001,
        (long)
            stubSettings
                .bulkMutateRowsSettings()
                .getBatchingSettings()
                .getFlowControlSettings()
                .getMaxOutstandingElementCount());

    assertEquals(
        100001,
        (long)
            stubSettings
                .bulkMutateRowsSettings()
                .getBatchingSettings()
                .getFlowControlSettings()
                .getMaxOutstandingRequestBytes());
  }

  @Test
  public void testUsingCredentialsFromBigtableOptions() throws Exception {
    Credentials fakeCredentials = Mockito.mock(Credentials.class);
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId("project")
            .setInstanceId("instance")
            .setAppProfileId("app-profile")
            .setCredentialOptions(
                CredentialOptions.UserSuppliedCredentialOptions.credential(fakeCredentials))
            .build();

    GcpOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);
    pipelineOptions.setGcpCredential(new TestCredential());

    BigtableConfig config = BigtableConfig.builder().setValidate(true).build();
    config = BigtableConfigTranslator.translateToBigtableConfig(config, options);

    BigtableDataSettings veneerSettings =
        BigtableConfigTranslator.translateToVeneerSettings(config, pipelineOptions);

    assertNotNull(veneerSettings.getStubSettings().getCredentialsProvider());

    assertEquals(
        fakeCredentials,
        veneerSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testUsingPipelineOptionsCredential() throws Exception {
    GcpOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);
    TestCredential testCredential = new TestCredential();
    pipelineOptions.setGcpCredential(testCredential);
    BigtableOptions options = BigtableOptions.builder().build();
    BigtableConfig config =
        BigtableConfig.builder()
            .setProjectId(ValueProvider.StaticValueProvider.of("project"))
            .setInstanceId(ValueProvider.StaticValueProvider.of("instance"))
            .setValidate(true)
            .build();

    config = BigtableConfigTranslator.translateToBigtableConfig(config, options);

    BigtableDataSettings veneerSettings =
        BigtableConfigTranslator.translateToVeneerSettings(config, pipelineOptions);

    assertNotNull(veneerSettings.getStubSettings().getCredentialsProvider());

    assertEquals(
        testCredential, veneerSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }
}
