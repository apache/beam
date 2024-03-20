package org.apache.beam.io.iceberg;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;

@SuppressWarnings("all")
public final class IcebergWriteResult implements POutput {

  private final Pipeline pipeline;
  @Nullable PCollection<Row> successfulInserts;
  @Nullable TupleTag<Row> successfulInsertsTag;

  @Nullable PCollection<Row> failedInserts;
  @Nullable TupleTag<Row> failedInsertsTag;

  @Nullable PCollection<KV<String, KV<String,DataFile>>> catalogUpdates;
  @Nullable TupleTag<KV<String,KV<String,DataFile>>> catalogUpdatesTag;

  @Nullable PCollection<KV<String, Snapshot>> snapshots;

  @Nullable TupleTag<KV<String,Snapshot>> snapshotsTag;


  public IcebergWriteResult(
      Pipeline pipeline,
      @Nullable PCollection<Row> successfulInserts,
      @Nullable PCollection<Row> failedInserts,
      @Nullable PCollection<KV<String, KV<String, DataFile>>> catalogUpdates,
      @Nullable PCollection<KV<String, Snapshot>> snapshots,
      @Nullable TupleTag<Row> successfulInsertsTag,
      @Nullable TupleTag<Row> failedInsertsTag,
      @Nullable TupleTag<KV<String, KV<String, DataFile>>> catalogUpdatesTag,
      @Nullable TupleTag<KV<String, Snapshot>> snapshotsTag) {
    this.pipeline = pipeline;
    this.successfulInserts = successfulInserts;
    this.catalogUpdates = catalogUpdates;
    this.snapshots = snapshots;

    this.successfulInsertsTag = successfulInsertsTag;
    this.failedInsertsTag = failedInsertsTag;
    this.catalogUpdatesTag = catalogUpdatesTag;
    this.snapshotsTag = snapshotsTag;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  public PCollection<Row> getSuccessfulInserts() {
    return successfulInserts;
  }

  public PCollection<Row> getFailedInserts() {
    return failedInserts;
  }

  @Override
  public Map<TupleTag<?>,PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> output = ImmutableMap.builder();
    if(successfulInsertsTag != null) {
      output.put(successfulInsertsTag, Preconditions.checkNotNull(successfulInserts));
    }
    if(failedInsertsTag != null) {
      output.put(failedInsertsTag,Preconditions.checkNotNull(failedInserts));
    }
    if(catalogUpdatesTag != null) {
      output.put(catalogUpdatesTag,Preconditions.checkNotNull(catalogUpdates));
    }
    if(snapshotsTag != null) {
      output.put(snapshotsTag,Preconditions.checkNotNull(snapshots));
    }

    return output.build();
  }

  @Override
  public void finishSpecifyingOutput(String transformName,
      PInput input,
      PTransform<?,?> transform) {
  }
}
