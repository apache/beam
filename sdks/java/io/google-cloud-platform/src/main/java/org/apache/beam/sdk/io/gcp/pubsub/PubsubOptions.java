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
package org.apache.beam.sdk.io.gcp.pubsub;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

/** Properties that can be set when using Google Cloud Pub/Sub with the Apache Beam SDK. */
@Description(
    "Options that are used to configure Google Cloud Pub/Sub. See "
        + "https://cloud.google.com/pubsub/docs/overview for details on Cloud Pub/Sub.")
public interface PubsubOptions
    extends ApplicationNameOptions, GcpOptions, PipelineOptions, StreamingOptions {

  /** Root URL for use with the Google Cloud Pub/Sub API. */
  @Description("Root URL for use with the Google Cloud Pub/Sub API")
  @Default.String("https://pubsub.googleapis.com")
  @Hidden
  String getPubsubRootUrl();

  void setPubsubRootUrl(String value);
}
