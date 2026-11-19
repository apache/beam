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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.v1.DirectedReadOptions;
import io.opentelemetry.api.GlobalOpenTelemetry;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerAccessorTest {

  private static SpannerOptions buildSpannerOptionsWithoutEnv(SpannerConfig config) {
    return SpannerAccessor.buildSpannerOptions(config, GlobalOpenTelemetry.get(), key -> null);
  }

  private static SpannerOptions buildSpannerOptionsWithEmulatorEnv(
      SpannerConfig config, @Nullable String emulatorHost) {
    return SpannerAccessor.buildSpannerOptions(
        config,
        GlobalOpenTelemetry.get(),
        key -> SpannerAccessor.SPANNER_EMULATOR_HOST_ENV_VAR.equals(key) ? emulatorHost : null);
  }

  private FakeServiceFactory serviceFactory;

  @Before
  public void setUp() throws Exception {

    serviceFactory = new FakeServiceFactory();
  }

  @Test
  public void testCreateOnlyOnce() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    SpannerAccessor acc1 = SpannerAccessor.getOrCreate(config1);
    SpannerAccessor acc2 = SpannerAccessor.getOrCreate(config1);
    SpannerAccessor acc3 = SpannerAccessor.getOrCreate(config1);

    acc1.close();
    acc2.close();
    acc3.close();

    // getDatabaseClient and close() only called once.
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(DatabaseId.of("project", "test1", "test1"));
    verify(serviceFactory.mockSpanner(), times(1)).close();
  }

  @Test
  public void testRefCountedSpannerAccessorDifferentDbsOnlyOnce() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();
    SpannerConfig config2 =
        config1.toBuilder()
            .setInstanceId(StaticValueProvider.of("test2"))
            .setDatabaseId(StaticValueProvider.of("test2"))
            .build();

    SpannerAccessor acc1a = SpannerAccessor.getOrCreate(config1);
    SpannerAccessor acc1b = SpannerAccessor.getOrCreate(config1);

    SpannerAccessor acc2a = SpannerAccessor.getOrCreate(config2);
    SpannerAccessor acc2b = SpannerAccessor.getOrCreate(config2);

    acc1a.close();
    acc2a.close();
    acc1b.close();
    acc2b.close();

    // getDatabaseClient called once each for the separate instances.
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(eq(DatabaseId.of("project", "test1", "test1")));
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(eq(DatabaseId.of("project", "test2", "test2")));
    verify(serviceFactory.mockSpanner(), times(2)).close();
  }

  @Test
  public void testCreateWithValidDatabaseRole() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .setDatabaseRole(StaticValueProvider.of("test-role"))
            .build();

    SpannerAccessor acc1 = SpannerAccessor.getOrCreate(config1);
    acc1.close();

    // getDatabaseClient and close() only called once.
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(DatabaseId.of("project", "test1", "test1"));
    verify(serviceFactory.mockSpanner(), times(1)).close();
  }

  @Test
  public void testCreateWithEmptyDatabaseRole() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .setDatabaseRole(StaticValueProvider.of(""))
            .build();

    SpannerAccessor acc1 = SpannerAccessor.getOrCreate(config1);
    acc1.close();

    // getDatabaseClient and close() only called once.
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(DatabaseId.of("project", "test1", "test1"));
    verify(serviceFactory.mockSpanner(), times(1)).close();
  }

  @Test
  public void testBuildSpannerOptionsWithCredential() {
    TestCredential testCredential = new TestCredential();
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test-instance"))
            .setDatabaseId(StaticValueProvider.of("test-db"))
            .setDatabaseRole(StaticValueProvider.of("test-role"))
            .setCredentials(StaticValueProvider.of(testCredential))
            .build();

    SpannerOptions options = buildSpannerOptionsWithoutEnv(config1);
    assertEquals("project", options.getProjectId());
    assertEquals("test-role", options.getDatabaseRole());
    assertEquals(testCredential, options.getCredentials());
  }

  @Test
  public void testBuildSpannerOptionsWithNoHost() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    assertEquals(SpannerConfig.DEFAULT_HOST, config1.getHostValue());
    SpannerOptions options = buildSpannerOptionsWithoutEnv(config1);
    assertEquals(SpannerConfig.DEFAULT_HOST, options.getHost());
  }

  @Test
  public void testBuildSpannerOptionsWithNullHost() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setHost((StaticValueProvider<String>) null)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    assertEquals(SpannerConfig.DEFAULT_HOST, config1.getHostValue());
    SpannerOptions options = buildSpannerOptionsWithoutEnv(config1);
    assertEquals(SpannerConfig.DEFAULT_HOST, options.getHost());
  }

  @Test
  public void testBuildSpannerOptionsWithNullHostValue() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setHost(StaticValueProvider.of((String) null))
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    assertEquals(SpannerConfig.DEFAULT_HOST, config1.getHostValue());
    SpannerOptions options = buildSpannerOptionsWithoutEnv(config1);
    assertEquals(SpannerConfig.DEFAULT_HOST, options.getHost());
  }

  @Test
  public void testBuildSpannerOptionsWithEmptyHost() {
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setHost(StaticValueProvider.of(""))
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    assertEquals(SpannerConfig.DEFAULT_HOST, config1.getHostValue());
    SpannerOptions options = buildSpannerOptionsWithoutEnv(config1);
    assertEquals(SpannerConfig.DEFAULT_HOST, options.getHost());
  }

  @Test
  public void testBuildSpannerOptionsWithCustomHost() {
    final String host = "https://alternative-host.example.org";
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setHost(StaticValueProvider.of(host))
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    assertEquals(host, config1.getHostValue());
    SpannerOptions options = buildSpannerOptionsWithoutEnv(config1);
    assertEquals(host, options.getHost());
  }

  @Test
  public void testBuildSpannerOptionsUsesEnvEmulatorHostWhenDefaultHost() {
    SpannerConfig config =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withInstanceId("test1")
            .withDatabaseId("test1");

    SpannerOptions optionsWithoutScheme =
        buildSpannerOptionsWithEmulatorEnv(config, "localhost:9010");
    assertEquals("http://localhost:9010", optionsWithoutScheme.getHost());
    assertEquals(NoCredentials.getInstance(), optionsWithoutScheme.getCredentials());

    SpannerOptions optionsWithScheme =
        buildSpannerOptionsWithEmulatorEnv(config, "http://localhost:9010");
    assertEquals("http://localhost:9010", optionsWithScheme.getHost());
    assertEquals(NoCredentials.getInstance(), optionsWithScheme.getCredentials());
  }

  @Test
  public void testBuildSpannerOptionsUsesEnvEmulatorHostWhenNullOrEmptyHost() {
    SpannerConfig nullProviderConfig =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setHost(null)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();
    SpannerConfig nullValueConfig =
        nullProviderConfig.toBuilder().setHost(StaticValueProvider.of(null)).build();
    SpannerConfig emptyValueConfig =
        nullProviderConfig.toBuilder().setHost(StaticValueProvider.of("")).build();

    for (SpannerConfig config :
        new SpannerConfig[] {nullProviderConfig, nullValueConfig, emptyValueConfig}) {
      SpannerOptions options = buildSpannerOptionsWithEmulatorEnv(config, "localhost:9010");
      assertEquals("http://localhost:9010", options.getHost());
      assertEquals(NoCredentials.getInstance(), options.getCredentials());
    }
  }

  @Test
  public void testBuildSpannerOptionsIgnoresEmptyEnvEmulatorHost() {
    SpannerConfig config =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withInstanceId("test1")
            .withDatabaseId("test1");

    SpannerOptions options = buildSpannerOptionsWithEmulatorEnv(config, "");
    assertEquals(SpannerConfig.DEFAULT_HOST, options.getHost());
  }

  @Test
  public void testBuildSpannerOptionsExplicitEmulatorHostOverridesEnvEmulatorHost() {
    SpannerConfig config =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withInstanceId("test1")
            .withDatabaseId("test1")
            .withEmulatorHost(StaticValueProvider.of("localhost:9999"));

    SpannerOptions options = buildSpannerOptionsWithEmulatorEnv(config, "localhost:9010");
    assertEquals("http://localhost:9999", options.getHost());
    assertEquals(NoCredentials.getInstance(), options.getCredentials());
  }

  @Test
  public void testBuildSpannerOptionsNullOrEmptyConfigEmulatorHostFallsBack() {
    TestCredential testCredential = new TestCredential();

    for (StaticValueProvider<String> emptyEmulatorProvider :
        Arrays.asList(StaticValueProvider.<String>of(null), StaticValueProvider.of(""))) {
      SpannerConfig config =
          SpannerConfig.create()
              .withServiceFactory(serviceFactory)
              .withProjectId("project")
              .withInstanceId("test1")
              .withDatabaseId("test1")
              .withCredentials(testCredential)
              .withEmulatorHost(emptyEmulatorProvider);

      SpannerOptions optionsWithEnv = buildSpannerOptionsWithEmulatorEnv(config, "localhost:9010");
      assertEquals("http://localhost:9010", optionsWithEnv.getHost());
      assertEquals(NoCredentials.getInstance(), optionsWithEnv.getCredentials());

      SpannerOptions optionsWithoutEnv = buildSpannerOptionsWithoutEnv(config);
      assertEquals(SpannerConfig.DEFAULT_HOST, optionsWithoutEnv.getHost());
      assertEquals(testCredential, optionsWithoutEnv.getCredentials());
    }
  }

  @Test
  public void testBuildSpannerOptionsCustomHostOverridesEnvEmulatorHost() {
    TestCredential testCredential = new TestCredential();
    String customHost = "https://alternative-host.example.org";

    SpannerConfig config =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withInstanceId("test1")
            .withDatabaseId("test1")
            .withCredentials(testCredential)
            .withHost(StaticValueProvider.of(customHost));

    SpannerOptions options = buildSpannerOptionsWithEmulatorEnv(config, "localhost:9010");
    assertEquals(customHost, options.getHost());
    assertEquals(testCredential, options.getCredentials());
  }

  @Test
  public void testBuildSpannerOptionsExperimentalHostOverridesEnvEmulatorHost() {
    SpannerConfig config =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withDatabaseId("test1")
            .withExperimentalHost("https://localhost:8080");

    SpannerOptions options = buildSpannerOptionsWithEmulatorEnv(config, "localhost:9010");
    assertEquals("https://localhost:8080", options.getHost());
    assertEquals(NoCredentials.getInstance(), options.getCredentials());
  }

  @Test
  public void testBuildSpannerOptionsEnvEmulatorHostWithCredentialsAndLocalChannelProvider() {
    TestCredential testCredential = new TestCredential();

    SpannerConfig config =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withInstanceId("test1")
            .withDatabaseId("test1")
            .withCredentials(testCredential)
            .withIsLocalChannelProvider(StaticValueProvider.of(true));

    SpannerOptions options = buildSpannerOptionsWithEmulatorEnv(config, "localhost:9010");
    assertEquals("http://localhost:9010", options.getHost());
    assertEquals(NoCredentials.getInstance(), options.getCredentials());
    assertTrue(options.getChannelProvider() instanceof LocalChannelProvider);
  }

  @Test
  public void testBuildSpannerOptionsDefaultEnvLookup() {
    SpannerConfig config =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withInstanceId("test1")
            .withDatabaseId("test1");

    SpannerOptions options = SpannerAccessor.buildSpannerOptions(config);
    String envEmulatorHost = System.getenv(SpannerAccessor.SPANNER_EMULATOR_HOST_ENV_VAR);
    if (envEmulatorHost != null && !envEmulatorHost.isEmpty()) {
      String expectedHost =
          envEmulatorHost.startsWith("http") ? envEmulatorHost : "http://" + envEmulatorHost;
      assertEquals(expectedHost, options.getHost());
      assertEquals(NoCredentials.getInstance(), options.getCredentials());
    } else {
      assertEquals(SpannerConfig.DEFAULT_HOST, options.getHost());
    }
  }

  @Test
  public void testBuildSpannerOptionsWithDirectedReadOptions() {
    DirectedReadOptions directedReadOptions =
        DirectedReadOptions.newBuilder()
            .setIncludeReplicas(
                DirectedReadOptions.IncludeReplicas.newBuilder()
                    .addReplicaSelections(
                        DirectedReadOptions.ReplicaSelection.newBuilder()
                            .setLocation("us-central1")
                            .setType(DirectedReadOptions.ReplicaSelection.Type.READ_ONLY)))
            .build();
    SpannerConfig config1 =
        SpannerConfig.create().toBuilder()
            .setServiceFactory(serviceFactory)
            .setDirectedReadOptions(StaticValueProvider.of(directedReadOptions))
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    SpannerOptions options = SpannerAccessor.buildSpannerOptions(config1);
    assertEquals(directedReadOptions, options.getDirectedReadOptions());
  }

  @Test
  public void testBuildSpannerOptionsWithDirectedReadOptionsJson() {
    String jsonString =
        "{\"includeReplicas\":{\"replicaSelections\":[{\"location\":\"us-east1\",\"type\":\"READ_WRITE\"}]}}";
    SpannerConfig config1 =
        SpannerConfig.create()
            .withServiceFactory(serviceFactory)
            .withProjectId("project")
            .withInstanceId("test1")
            .withDatabaseId("test1")
            .withDirectedReadOptions(jsonString);

    SpannerOptions options = SpannerAccessor.buildSpannerOptions(config1);
    assertEquals(
        DirectedReadOptions.ReplicaSelection.Type.READ_WRITE,
        options.getDirectedReadOptions().getIncludeReplicas().getReplicaSelections(0).getType());
    assertEquals(
        "us-east1",
        options
            .getDirectedReadOptions()
            .getIncludeReplicas()
            .getReplicaSelections(0)
            .getLocation());
  }
}
