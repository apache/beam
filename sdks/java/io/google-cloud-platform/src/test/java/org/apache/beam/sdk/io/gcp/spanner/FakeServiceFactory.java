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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.mockito.Matchers;

/**
 * A serialization friendly type service factory that maintains a mock {@link Spanner} and {@link
 * DatabaseClient}.
 */
class FakeServiceFactory implements ServiceFactory<Spanner, SpannerOptions>, Serializable {

  // Marked as static so they could be returned by serviceFactory, which is serializable.
  private static final Object lock = new Object();

  @GuardedBy("lock")
  private static final List<Spanner> mockSpanners = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<DatabaseClient> mockDatabaseClients = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<BatchClient> mockBatchClients = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<InstanceAdminClient> mockAdminClients = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<Instance> mockInstances = new ArrayList<>();

  @GuardedBy("lock")
  private static int count = 0;

  private final int index;

  public FakeServiceFactory() {
    synchronized (lock) {
      index = count++;
      mockSpanners.add(mock(Spanner.class, withSettings().serializable()));
      mockDatabaseClients.add(mock(DatabaseClient.class, withSettings().serializable()));
      mockBatchClients.add(mock(BatchClient.class, withSettings().serializable()));
      mockAdminClients.add(mock(InstanceAdminClient.class, withSettings().serializable()));
      mockInstances.add(mock(Instance.class, withSettings().serializable()));
    }
    when(mockAdminClient().getInstance(Matchers.any(String.class))).thenReturn(mockInstance());
    when(mockSpanner().getDatabaseClient(Matchers.any(DatabaseId.class)))
        .thenReturn(mockDatabaseClient());
    when(mockSpanner().getBatchClient(Matchers.any(DatabaseId.class)))
        .thenReturn(mockBatchClient());
    when(mockSpanner().getInstanceAdminClient()).thenReturn(mockAdminClient());
  }

  DatabaseClient mockDatabaseClient() {
    synchronized (lock) {
      return mockDatabaseClients.get(index);
    }
  }

  BatchClient mockBatchClient() {
    synchronized (lock) {
      return mockBatchClients.get(index);
    }
  }

  Spanner mockSpanner() {
    synchronized (lock) {
      return mockSpanners.get(index);
    }
  }

  InstanceAdminClient mockAdminClient() {
    synchronized (lock) {
      return mockAdminClients.get(index);
    }
  }

  Instance mockInstance() {
    synchronized (lock) {
      return mockInstances.get(index);
    }
  }

  @Override
  public Spanner create(SpannerOptions serviceOptions) {
    return mockSpanner();
  }
}
