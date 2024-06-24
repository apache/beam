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
import static org.junit.Assert.assertThrows;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.ExperimentalOptions;
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
    BigtableReadOptions fromBigtableOptions =
        BigtableConfigTranslator.translateToBigtableReadOptions(readOptions, options);

    assertNotNull(fromBigtableOptions.getAttemptTimeout());
    assertNotNull(fromBigtableOptions.getOperationTimeout());

    assertEquals(org.joda.time.Duration.millis(100), fromBigtableOptions.getAttemptTimeout());
    assertEquals(org.joda.time.Duration.millis(1000), fromBigtableOptions.getOperationTimeout());
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

    BigtableWriteOptions fromBigtableOptions =
        BigtableConfigTranslator.translateToBigtableWriteOptions(writeOptions, options);

    assertNotNull(fromBigtableOptions.getAttemptTimeout());
    assertNotNull(fromBigtableOptions.getOperationTimeout());
    assertNotNull(fromBigtableOptions.getMaxBytesPerBatch());
    assertNotNull(fromBigtableOptions.getMaxElementsPerBatch());
    assertNotNull(fromBigtableOptions.getMaxOutstandingElements());
    assertNotNull(fromBigtableOptions.getMaxOutstandingBytes());

    assertEquals(org.joda.time.Duration.millis(200), fromBigtableOptions.getAttemptTimeout());
    assertEquals(org.joda.time.Duration.millis(2000), fromBigtableOptions.getOperationTimeout());
    assertEquals(20, (long) fromBigtableOptions.getMaxBytesPerBatch());
    assertEquals(100, (long) fromBigtableOptions.getMaxElementsPerBatch());
    assertEquals(5 * 100, (long) fromBigtableOptions.getMaxOutstandingElements());
    assertEquals(5 * 20, (long) fromBigtableOptions.getMaxOutstandingBytes());
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
            config, readOptions, null, pipelineOptions);

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
  public void testReadOptionsOverride() throws Exception {
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

    BigtableOptions options =
        BigtableOptions.builder()
            .setCallOptionsConfig(
                CallOptionsConfig.builder()
                    .setReadRowsRpcAttemptTimeoutMs(200)
                    .setReadRowsRpcTimeoutMs(2001)
                    .build())
            .build();

    BigtableReadOptions fromBigtableOptions =
        BigtableConfigTranslator.translateToBigtableReadOptions(readOptions, options);

    PipelineOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);

    BigtableDataSettings settings =
        BigtableConfigTranslator.translateReadToVeneerSettings(
            config, readOptions, fromBigtableOptions, pipelineOptions);

    assertEquals(
        Duration.ofMillis(101),
        settings.getStubSettings().readRowsSettings().getRetrySettings().getInitialRpcTimeout());
    assertEquals(
        Duration.ofMillis(1001),
        settings.getStubSettings().readRowsSettings().getRetrySettings().getTotalTimeout());
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
            config, writeOptions, null, pipelineOptions);

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
  public void testWriteOptionsOverride() throws Exception {
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

    BigtableOptions options =
        BigtableOptions.builder()
            .setCallOptionsConfig(
                CallOptionsConfig.builder()
                    .setMutateRpcAttemptTimeoutMs(456)
                    .setMutateRpcTimeoutMs(678)
                    .build())
            .setBulkOptions(
                BulkOptions.builder()
                    .setMaxInflightRpcs(15)
                    .setBulkMaxRowKeyCount(100)
                    .setBulkMaxRequestSize(200)
                    .build())
            .build();

    PipelineOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);

    BigtableDataSettings settings =
        BigtableConfigTranslator.translateWriteToVeneerSettings(
            config,
            writeOptions,
            BigtableConfigTranslator.translateToBigtableWriteOptions(writeOptions, options),
            pipelineOptions);

    EnhancedBigtableStubSettings stubSettings = settings.getStubSettings();

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

  @Test
  public void testSettingsOverride() throws Exception {
    ExperimentalOptions pipelineOptions = PipelineOptionsFactory.as(ExperimentalOptions.class);
    pipelineOptions.setExperiments(
        Arrays.asList(
            String.format(
                "%s=%s",
                BigtableConfigTranslator.BIGTABLE_SETTINGS_OVERRIDE,
                MySettingsOverride.class.getName())));
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

    assertEquals("test-endpoint:123", veneerSettings.getStubSettings().getEndpoint());
    assertEquals(
        Duration.ofSeconds(1),
        veneerSettings
            .getStubSettings()
            .readRowsSettings()
            .getRetrySettings()
            .getInitialRpcTimeout());
    assertEquals(
        Duration.ofSeconds(1),
        veneerSettings.getStubSettings().readRowsSettings().getRetrySettings().getMaxRpcTimeout());
    assertEquals(
        Duration.ofSeconds(2),
        veneerSettings.getStubSettings().readRowsSettings().getRetrySettings().getTotalTimeout());
  }

  @Test
  public void testSettingsOverrideNoOverrideClass() {
    ExperimentalOptions pipelineOptions = PipelineOptionsFactory.as(ExperimentalOptions.class);
    pipelineOptions.setExperiments(
        Arrays.asList(
            String.format(
                "%s=%s", BigtableConfigTranslator.BIGTABLE_SETTINGS_OVERRIDE, "FakeSettings")));

    BigtableOptions options = BigtableOptions.builder().build();
    BigtableConfig config =
        BigtableConfig.builder()
            .setProjectId(ValueProvider.StaticValueProvider.of("project"))
            .setInstanceId(ValueProvider.StaticValueProvider.of("instance"))
            .setValidate(true)
            .build();

    config = BigtableConfigTranslator.translateToBigtableConfig(config, options);

    BigtableConfig finalConfig = config;
    assertThrows(
        IllegalArgumentException.class,
        () -> BigtableConfigTranslator.translateToVeneerSettings(finalConfig, pipelineOptions));
  }

  @Test
  public void testSettingsOverrideWrongType() {
    ExperimentalOptions pipelineOptions = PipelineOptionsFactory.as(ExperimentalOptions.class);
    Function<String, String> mockFunction = Mockito.mock(Function.class);
    pipelineOptions.setExperiments(
        Arrays.asList(
            String.format(
                "%s=%s",
                BigtableConfigTranslator.BIGTABLE_SETTINGS_OVERRIDE,
                mockFunction.getClass().getName())));

    BigtableOptions options = BigtableOptions.builder().build();
    BigtableConfig config =
        BigtableConfig.builder()
            .setProjectId(ValueProvider.StaticValueProvider.of("project"))
            .setInstanceId(ValueProvider.StaticValueProvider.of("instance"))
            .setValidate(true)
            .build();

    config = BigtableConfigTranslator.translateToBigtableConfig(config, options);

    BigtableConfig finalConfig = config;
    assertThrows(
        IllegalStateException.class,
        () -> BigtableConfigTranslator.translateToVeneerSettings(finalConfig, pipelineOptions));
  }

  public static class MySettingsOverride
      implements BiFunction<
          BigtableDataSettings.Builder, PipelineOptions, BigtableDataSettings.Builder> {

    @Override
    public BigtableDataSettings.Builder apply(
        BigtableDataSettings.Builder builder, PipelineOptions pipelineOptions) {
      builder
          .stubSettings()
          .setEndpoint("test-endpoint:123")
          .readRowsSettings()
          .setRetrySettings(
              RetrySettings.newBuilder()
                  .setInitialRpcTimeout(Duration.ofSeconds(1))
                  .setMaxRpcTimeout(Duration.ofSeconds(1))
                  .setTotalTimeout(Duration.ofSeconds(2))
                  .build());
      return builder;
    }
  }
}
