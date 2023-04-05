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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class ManagedFactoryImpl<T extends AutoCloseable> implements ManagedFactory<T> {

  private final SerializableFunction<SubscriptionPartition, T> newInstance;

  @GuardedBy("this")
  private final Map<SubscriptionPartition, T> instances = new HashMap<>();

  ManagedFactoryImpl(SerializableFunction<SubscriptionPartition, T> newInstance) {
    this.newInstance = newInstance;
  }

  @Override
  public synchronized T create(SubscriptionPartition subscriptionPartition) {
    return instances.computeIfAbsent(subscriptionPartition, newInstance::apply);
  }

  @Override
  public synchronized void close() throws Exception {
    @Nullable Exception e = null;
    for (AutoCloseable c : instances.values()) {
      try {
        c.close();
      } catch (Exception e2) {
        if (e == null) {
          e = e2;
        } else {
          e.addSuppressed(e2);
        }
      }
    }
    if (e != null) {
      throw e;
    }
  }
}
