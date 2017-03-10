package org.beam.sdk.java.sql.planner;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.beam.sdk.java.sql.rel.BeamRelNode;
import org.beam.sdk.java.sql.schema.BaseBeamTable;
import org.beam.sdk.java.sql.schema.BeamSQLRow;
import org.beam.sdk.java.sql.transform.BeamSQLOutputToConsoleFn;

import io.ebay.rheos.schema.event.RheosEvent;

/**
 * {@link BeamPipelineCreator} converts a {@link BeamRelNode} tree, into a Beam pipeline.
 * 
 */
public class BeamPipelineCreator {
  private Map<String, BaseBeamTable> sourceTables;
  private PCollection<BeamSQLRow> latestStream;

  private PipelineOptions options;

  private Pipeline pipeline;

  private boolean hasPersistent = false;

  public BeamPipelineCreator(Map<String, BaseBeamTable> sourceTables) {
    this.sourceTables = sourceTables;

    options = PipelineOptionsFactory.fromArgs(new String[] {}).withValidation()
        .as(PipelineOptions.class); // FlinkPipelineOptions.class
    options.setJobName("BeamPlanCreator");

    pipeline = Pipeline.create(options);
  }

  public void runJob() {
    if (!hasPersistent) {
      latestStream.apply("emit_to_console", ParDo.of(new BeamSQLOutputToConsoleFn("emit_to_console")));
    }

    pipeline.run();
  }

  public PCollection<BeamSQLRow> getLatestStream() {
    return latestStream;
  }

  public void setLatestStream(PCollection<BeamSQLRow> latestStream) {
    this.latestStream = latestStream;
  }

  public Map<String, BaseBeamTable> getSourceTables() {
    return sourceTables;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public boolean isHasPersistent() {
    return hasPersistent;
  }

  public void setHasPersistent(boolean hasPersistent) {
    this.hasPersistent = hasPersistent;
  }

}
