package org.apache.beam.io.iceberg;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;

public final class IcebergWriteResult implements POutput {

  Pipeline pipeline;
  PCollection<Row> successfulInserts;

  PCollection<DataFile> datafiles;

  public IcebergWriteResult(
      Pipeline pipeline,
      PCollection<Row> successfulInserts,
      PCollection<DataFile> datafiles) {
    this.pipeline = pipeline;
    this.successfulInserts = successfulInserts;
    this.datafiles = datafiles;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>,PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> output = ImmutableMap.builder();
    return output.build();
  }

  @Override
  public void finishSpecifyingOutput(String transformName,
      PInput input,
      PTransform<?,?> transform) {
  }
}
