package org.beam.sdk.java.sql.schema.kafka;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
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
import org.beam.sdk.java.sql.examples.RheosSinkTransform;
import org.beam.sdk.java.sql.examples.RheosSourceTransform;
import org.beam.sdk.java.sql.schema.BaseBeamTable;
import org.beam.sdk.java.sql.schema.BeamIOType;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class BeamKafkaTable extends BaseBeamTable<KV<byte[], byte[]>> implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -634715473399906527L;

  private String bootstrapServers;
  private List<String> topics;
  private Map<String, Object> configUpdates;

  protected BeamKafkaTable(RelProtoDataType protoRowType,
      PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSQLRow>> sourceConverter,
      PTransform<PCollection<BeamSQLRow>, PCollection<KV<byte[], byte[]>>> sinkConcerter) {
    super(protoRowType, sourceConverter, sinkConcerter);
  }

  public BeamKafkaTable(RelProtoDataType protoRowType,
      PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSQLRow>> sourceConverter,
      PTransform<PCollection<BeamSQLRow>, PCollection<KV<byte[], byte[]>>> sinkConcerter,
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
  public PTransform<? super PBegin, PCollection<KV<byte[], byte[]>>> buildReadTransform() {
    return KafkaIO.<byte[], byte[]>read().withBootstrapServers(this.bootstrapServers)
        .withTopics(this.topics).updateConsumerProperties(configUpdates)
        .withKeyCoder(ByteArrayCoder.of()).withValueCoder(ByteArrayCoder.of())
        .withoutMetadata();
  }

  @Override
  public PTransform<? super PCollection<KV<byte[], byte[]>>, PDone> buildWriteTransform() {
    checkArgument(topics != null && topics.size() == 1,
        "Only one topic can be acceptable as output.");

    return new PTransform<PCollection<KV<byte[], byte[]>>, PDone>() {
      /**
       * 
       */
      private static final long serialVersionUID = 1136964183593770265L;

      @Override
      public PDone expand(PCollection<KV<byte[], byte[]>> input) {
        return input.apply("writeToKafka",
                KafkaIO.<byte[], byte[]>write().withBootstrapServers(bootstrapServers)
                    .withTopic(topics.get(0)).withKeyCoder(ByteArrayCoder.of())
                    .withValueCoder(ByteArrayCoder.of()));
      }
    };
  }

}
