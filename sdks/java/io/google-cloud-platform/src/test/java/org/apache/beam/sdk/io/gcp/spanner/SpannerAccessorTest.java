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
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerAccessorTest {

  private FakeServiceFactory serviceFactory;

  @Before
  public void setUp() throws Exception {

    serviceFactory = new FakeServiceFactory();
  }

  @Test
  public void testCreateOnlyOnce() {
    SpannerConfig config1 =
        SpannerConfig.create()
            .toBuilder()
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
        SpannerConfig.create()
            .toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();
    SpannerConfig config2 =
        config1
            .toBuilder()
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
        SpannerConfig.create()
            .toBuilder()
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
        SpannerConfig.create()
            .toBuilder()
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
        SpannerConfig.create()
            .toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test-instance"))
            .setDatabaseId(StaticValueProvider.of("test-db"))
            .setDatabaseRole(StaticValueProvider.of("test-role"))
            .setCredentials(StaticValueProvider.of(testCredential))
            .build();

    SpannerOptions options = SpannerAccessor.buildSpannerOptions(config1);
    assertEquals("project", options.getProjectId());
    assertEquals("test-role", options.getDatabaseRole());
    assertEquals(testCredential, options.getCredentials());
    assertNotNull(options.getSessionPoolOptions());
  }
}
