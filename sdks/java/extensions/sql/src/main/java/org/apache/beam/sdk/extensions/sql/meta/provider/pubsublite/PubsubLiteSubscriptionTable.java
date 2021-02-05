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
        .apply("Read Pub/Sub Lite", PubsubLiteIO.read(SubscriberOptions.newBuilder().setSubscriptionPath(subscription).build()))
        .apply("Transform to Row", transform);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("You cannot write to a Pub/Sub Lite subscription: you must write to a topic.");
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
