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

import org.apache.beam.sdk.annotations.Experimental;

/**
 * I/O transforms for reading from Google Pub/Sub Lite.
 *
 * <p>For the differences between this and Google Pub/Sub, please refer to the <a
 * href="https://cloud.google.com/pubsub/docs/choosing-pubsub-or-lite">product documentation</a>.
 *
 * <p>I/O methods are implemented in external pubsublite-beam-io package, please refer to <a
 * href="https://googleapis.dev/java/google-cloud-pubsublite/latest/com/google/cloud/pubsublite/beam/PubsubLiteIO.html">that
 * package's documentation.</a>
 */
@Experimental
public final class PubsubLiteIO extends com.google.cloud.pubsublite.beam.PubsubLiteIO {}
