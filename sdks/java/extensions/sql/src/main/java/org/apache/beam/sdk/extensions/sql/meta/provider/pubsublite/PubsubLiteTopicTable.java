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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsublite;

import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

class PubsubLiteTopicTable extends SchemaBaseBeamTable {
  private final TopicPath topic;
  private final PTransform<PCollection<? extends Row>, PCollection<PubSubMessage>> transform;

  PubsubLiteTopicTable(
      Schema schema,
      TopicPath topic,
      PTransform<PCollection<? extends Row>, PCollection<PubSubMessage>> transform) {
    super(schema);
    this.topic = topic;
    this.transform = transform;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    throw new UnsupportedOperationException(
        "You cannot read from a Pub/Sub Lite topic: you must create a subscription first.");
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input
        .apply("Transform to PubSubMessage", transform)
        .apply(
            "Write Pub/Sub Lite",
            PubsubLiteIO.write(PublisherOptions.newBuilder().setTopicPath(topic).build()));
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.UNBOUNDED_UNKNOWN;
  }
}
