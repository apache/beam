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

import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

class PubsubLiteSubscriptionTable extends SchemaBaseBeamTable {
  private final SubscriptionPath subscription;
  private final PTransform<PCollection<? extends SequencedMessage>, PCollection<Row>> transform;

  PubsubLiteSubscriptionTable(
      Schema schema,
      SubscriptionPath subscription,
      PTransform<PCollection<? extends SequencedMessage>, PCollection<Row>> transform) {
    super(schema);
    this.subscription = subscription;
    this.transform = transform;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(
            "Read Pub/Sub Lite",
            PubsubLiteIO.read(
                SubscriberOptions.newBuilder().setSubscriptionPath(subscription).build()))
        .apply("Transform to Row", transform);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException(
        "You cannot write to a Pub/Sub Lite subscription: you must write to a topic.");
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
