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

package org.apache.beam.sdk.nexmark.sinks.pubsub;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.utils.PubsubUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Sink which sends formatted results to Pubsub.
 */
public class PubsubResultsSink {

  /**
   * Send {@code formattedResults} to Pubsub.
   */
  public static PTransform<PCollection<String>, PDone> createPubsubSink(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    String shortTopic = PubsubUtils.shortTopic(options, queryName, now);
    NexmarkUtils.console("Writing results to Pubsub %s", shortTopic);

    PubsubIO.Write<String> pubsubSink =
        PubsubIO
            .writeStrings()
            .to(shortTopic)
            .withIdAttribute(NexmarkUtils.PUBSUB_ID);

    if (!configuration.usePubsubPublishTime) {
      pubsubSink = pubsubSink.withTimestampAttribute(NexmarkUtils.PUBSUB_TIMESTAMP);
    }

    return pubsubSink;
  }

}
