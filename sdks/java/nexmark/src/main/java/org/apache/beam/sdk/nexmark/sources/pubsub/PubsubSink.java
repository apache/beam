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

package org.apache.beam.sdk.nexmark.sources.pubsub;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Sends generated events to Pubsub. These events can later be used by
 * {@link PubsubEventsSource}.
 */
public class PubsubSink extends PTransform<PCollection<Event>, PDone> {
  private String queryName;
  private String shortTopicName;
  private boolean usePubsubPublishTime;

  public PubsubSink(String queryName,
                    String shortTopicName,
                    boolean usePubsubPublishTime) {
    this.queryName = queryName;
    this.shortTopicName = shortTopicName;
    this.usePubsubPublishTime = usePubsubPublishTime;
  }

  @Override
  public PDone expand(PCollection<Event> events) {
    PubsubIO.Write<PubsubMessage> pubsubIO =
        PubsubIO
            .writeMessages()
            .to(shortTopicName)
            .withIdAttribute(NexmarkUtils.PUBSUB_ID);

    if (!usePubsubPublishTime) {
      pubsubIO = pubsubIO.withTimestampAttribute(NexmarkUtils.PUBSUB_TIMESTAMP);
    }

    return events
        .apply(queryName + ".EventToPubsubMessage", MessageTransforms.eventToMessage())
        .apply(queryName + ".WritePubsubEvents", pubsubIO);
  }
}
