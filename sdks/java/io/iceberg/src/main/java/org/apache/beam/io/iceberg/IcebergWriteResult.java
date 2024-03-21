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
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;

@SuppressWarnings("all")
public final class IcebergWriteResult<ElementT> implements POutput {

  private final Pipeline pipeline;
  @Nullable PCollection<ElementT> successfulInserts;
  @Nullable TupleTag<ElementT> successfulInsertsTag;

  @Nullable PCollection<KV<String, KV<String,DataFile>>> catalogUpdates;
  @Nullable TupleTag<KV<String,KV<String,DataFile>>> catalogUpdatesTag;

  @Nullable PCollection<KV<String, Snapshot>> snapshots;

  @Nullable TupleTag<KV<String,Snapshot>> snapshotsTag;


  public IcebergWriteResult(
      Pipeline pipeline,

      @Nullable PCollection<ElementT> successfulInserts,
      @Nullable PCollection<KV<String, KV<String, DataFile>>> catalogUpdates,
      @Nullable PCollection<KV<String, Snapshot>> snapshots,

      @Nullable TupleTag<ElementT> successfulInsertsTag,
      @Nullable TupleTag<KV<String, KV<String, DataFile>>> catalogUpdatesTag,
      @Nullable TupleTag<KV<String, Snapshot>> snapshotsTag) {
    this.pipeline = pipeline;
    this.successfulInserts = successfulInserts;
    this.catalogUpdates = catalogUpdates;
    this.snapshots = snapshots;

    this.successfulInsertsTag = successfulInsertsTag;
    this.catalogUpdatesTag = catalogUpdatesTag;
    this.snapshotsTag = snapshotsTag;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  public PCollection<ElementT> getSuccessfulInserts() {
    return successfulInserts;
  }


  @Override
  public Map<TupleTag<?>,PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> output = ImmutableMap.builder();
    if(successfulInsertsTag != null) {
      output.put(successfulInsertsTag, Preconditions.checkNotNull(successfulInserts));
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
