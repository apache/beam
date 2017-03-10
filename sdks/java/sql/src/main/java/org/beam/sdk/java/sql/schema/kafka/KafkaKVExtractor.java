package org.beam.sdk.java.sql.schema.kafka;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class KafkaKVExtractor extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<KV<byte[], byte[]>>>{

  /**
   * 
   */
  private static final long serialVersionUID = 4802754456560670375L;

  @Override
  public PCollection<KV<byte[], byte[]>> expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
    return input.apply("toKafkaKV",
        ParDo.of(new DoFn<KafkaRecord<byte[], byte[]>, KV<byte[], byte[]>>() {
          /**
           * 
           */
          private static final long serialVersionUID = 6265036192598208789L;

          @ProcessElement
          public void processElement(ProcessContext ctx) {
            ctx.output(ctx.element().getKV());
          }
        }));
  }

}
