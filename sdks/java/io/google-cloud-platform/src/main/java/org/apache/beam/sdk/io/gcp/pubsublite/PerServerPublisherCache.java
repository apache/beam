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
package org.apache.beam.sdk.io.gcp.pubsublite;

/**
 * A shared cache per-worker instance of Pub/Sub Lite publishers.
 *
 * <p>Pub/Sub Lite publishers connect to all available partitions: it would be a pessimization for
 * all instances of the PubsubLiteSink to do this.
 */
final class PerServerPublisherCache {
  private PerServerPublisherCache() {}

  static final PublisherCache PUBLISHER_CACHE = new PublisherCache();
}
