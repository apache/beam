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

import java.io.Serializable;

/**
 * A ManagedBacklogReaderFactory produces TopicBacklogReaders and tears down any produced readers
 * when it is itself closed.
 *
 * <p>close() should never be called on produced readers.
 */
public interface ManagedBacklogReaderFactory extends AutoCloseable, Serializable {
  TopicBacklogReader newReader(SubscriptionPartition subscriptionPartition);

  @Override
  void close();
}
