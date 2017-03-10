package org.beam.sdk.java.sql.examples;

import java.io.IOException;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.beam.sdk.java.sql.schema.BeamSQLRecordType;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class RheosSinkTransform extends PTransform<PCollection<BeamSQLRow>, PCollection<KV<byte[], byte[]>>>{
  /**
   * 
   */
  private static final long serialVersionUID = 6589679229923907701L;
  private BeamSQLRecordType recordType;
  

  public RheosSinkTransform(BeamSQLRecordType recordType) {
    super();
    this.recordType = recordType;
  }


  @Override
  public PCollection<KV<byte[], byte[]>> expand(PCollection<BeamSQLRow> input) {
    return input.apply("toKafkaRecord", ParDo.of(new DoFn<BeamSQLRow, KV<byte[], byte[]>>(){
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        c.output(KV.of(new byte[]{}, c.element().getDataMap().toString().getBytes()));
      }
    }));
  }

}
