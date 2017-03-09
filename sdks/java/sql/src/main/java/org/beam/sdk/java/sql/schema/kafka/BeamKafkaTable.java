package org.beam.sdk.java.sql.schema.kafka;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.beam.sdk.java.sql.schema.BaseBeamTable;
import org.beam.sdk.java.sql.schema.BeamIOType;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class BeamKafkaTable extends BaseBeamTable<KafkaRecord<byte[], byte[]>> {

  /**
   * 
   */
  private static final long serialVersionUID = -634715473399906527L;

  private String bootstrapServers;
  private List<String> topics;
  private Map<String, Object> configUpdates;

  private BeamKafkaTable(RelProtoDataType protoRowType,
      PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<BeamSQLRow>> sourceConverter,
      PTransform<PCollection<BeamSQLRow>, PCollection<KafkaRecord<byte[], byte[]>>> sinkConcerter) {
    super(protoRowType, sourceConverter, sinkConcerter);
  }

  public BeamKafkaTable(RelProtoDataType protoRowType,
      PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<BeamSQLRow>> sourceConverter,
      PTransform<PCollection<BeamSQLRow>, PCollection<KafkaRecord<byte[], byte[]>>> sinkConcerter,
      String bootstrapServers, List<String> topics) {
    super(protoRowType, sourceConverter, sinkConcerter);
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
  }

  public BeamKafkaTable updateConsumerProperties(Map<String, Object> configUpdates) {
    this.configUpdates = configUpdates;
    return this;
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  @Override
  public PTransform<? super PBegin, PCollection<KafkaRecord<byte[], byte[]>>> buildReadTransform() {
    return KafkaIO.<byte[], byte[]>read().withBootstrapServers(this.bootstrapServers)
        .withTopics(this.topics).updateConsumerProperties(configUpdates)
        .withKeyCoder(ByteArrayCoder.of()).withValueCoder(ByteArrayCoder.of());
  }

  @Override
  public PTransform<? super PCollection<KafkaRecord<byte[], byte[]>>, PDone> buildWriteTransform() {
    checkArgument(topics != null && topics.size() == 0,
        "Only one topic can be acceptable as output.");

    return new PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PDone>() {
      /**
       * 
       */
      private static final long serialVersionUID = 1136964183593770265L;

      @Override
      public PDone expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
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
            })).apply("writeToKafka",
                KafkaIO.<byte[], byte[]>write().withBootstrapServers(bootstrapServers)
                    .withTopic(topics.get(0)).withKeyCoder(ByteArrayCoder.of())
                    .withValueCoder(ByteArrayCoder.of()));
      }
    };
  }

}
