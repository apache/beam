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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class KafkaReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<KafkaReadSchemaTransformConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaReadSchemaTransformProvider.class);

  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

  @Override
  protected Class<KafkaReadSchemaTransformConfiguration> configurationClass() {
    return KafkaReadSchemaTransformConfiguration.class;
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  @Override
  protected SchemaTransform from(KafkaReadSchemaTransformConfiguration configuration) {
    return new KafkaReadSchemaTransform(configuration);
  }

  public static SerializableFunction<byte[], Row> getRawBytesToRowFunction(Schema rawSchema) {
    return new SimpleFunction<byte[], Row>() {
      @Override
      public Row apply(byte[] input) {
        return Row.withSchema(rawSchema).addValue(input).build();
      }
    };
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:kafka_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Lists.newArrayList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("output", "errors");
  }

  static class KafkaReadSchemaTransform extends SchemaTransform {
    private final KafkaReadSchemaTransformConfiguration configuration;

    KafkaReadSchemaTransform(KafkaReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically
        return SchemaRegistry.createDefault()
            .getToRowFunction(KafkaReadSchemaTransformConfiguration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      configuration.validate();

      final String inputSchema = configuration.getSchema();
      final int groupId = configuration.hashCode() % Integer.MAX_VALUE;
      final String autoOffsetReset =
          MoreObjects.firstNonNull(configuration.getAutoOffsetResetConfig(), "latest");

      Map<String, Object> consumerConfigs =
          new HashMap<>(
              MoreObjects.firstNonNull(configuration.getConsumerConfigUpdates(), new HashMap<>()));
      consumerConfigs.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "kafka-read-provider-" + groupId);
      consumerConfigs.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
      consumerConfigs.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
      consumerConfigs.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

      String format = configuration.getFormat();
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      SerializableFunction<byte[], Row> valueMapper;
      Schema beamSchema;

      String confluentSchemaRegUrl = configuration.getConfluentSchemaRegistryUrl();
      if (confluentSchemaRegUrl != null) {
        final String confluentSchemaRegSubject =
            checkArgumentNotNull(configuration.getConfluentSchemaRegistrySubject());
        KafkaIO.Read<byte[], GenericRecord> kafkaRead =
            KafkaIO.<byte[], GenericRecord>read()
                .withTopic(configuration.getTopic())
                .withConsumerFactoryFn(new ConsumerFactoryWithGcsTrustStores())
                .withBootstrapServers(configuration.getBootstrapServers())
                .withConsumerConfigUpdates(consumerConfigs)
                .withKeyDeserializer(ByteArrayDeserializer.class)
                .withValueDeserializer(
                    ConfluentSchemaRegistryDeserializerProvider.of(
                        confluentSchemaRegUrl, confluentSchemaRegSubject));
        Integer maxReadTimeSeconds = configuration.getMaxReadTimeSeconds();
        if (maxReadTimeSeconds != null) {
          kafkaRead = kafkaRead.withMaxReadTime(Duration.standardSeconds(maxReadTimeSeconds));
        }

        PCollection<GenericRecord> kafkaValues =
            input.getPipeline().apply(kafkaRead.withoutMetadata()).apply(Values.create());

        assert kafkaValues.getCoder().getClass() == AvroCoder.class;
        AvroCoder<GenericRecord> coder = (AvroCoder<GenericRecord>) kafkaValues.getCoder();
        kafkaValues = kafkaValues.setCoder(AvroUtils.schemaCoder(coder.getSchema()));
        return PCollectionRowTuple.of("output", kafkaValues.apply(Convert.toRows()));
      }

      if ("RAW".equals(format)) {
        beamSchema = Schema.builder().addField("payload", Schema.FieldType.BYTES).build();
        valueMapper = getRawBytesToRowFunction(beamSchema);
      } else if ("PROTO".equals(format)) {
        String fileDescriptorPath = configuration.getFileDescriptorPath();
        String messageName = checkArgumentNotNull(configuration.getMessageName());
        if (fileDescriptorPath != null) {
          beamSchema = ProtoByteUtils.getBeamSchemaFromProto(fileDescriptorPath, messageName);
          valueMapper = ProtoByteUtils.getProtoBytesToRowFunction(fileDescriptorPath, messageName);
        } else {
          beamSchema =
              ProtoByteUtils.getBeamSchemaFromProtoSchema(
                  checkArgumentNotNull(inputSchema), messageName);
          valueMapper =
              ProtoByteUtils.getProtoBytesToRowFromSchemaFunction(
                  checkArgumentNotNull(inputSchema), messageName);
        }
      } else if ("JSON".equals(format)) {
        beamSchema = JsonUtils.beamSchemaFromJsonSchema(checkArgumentNotNull(inputSchema));
        valueMapper = JsonUtils.getJsonBytesToRowFunction(beamSchema);
      } else {
        beamSchema =
            AvroUtils.toBeamSchema(
                new org.apache.avro.Schema.Parser().parse(checkArgumentNotNull(inputSchema)));
        valueMapper = AvroUtils.getAvroBytesToRowFunction(beamSchema);
      }

      KafkaIO.Read<byte[], byte[]> kafkaRead =
          KafkaIO.readBytes()
              .withConsumerConfigUpdates(consumerConfigs)
              .withConsumerFactoryFn(new ConsumerFactoryWithGcsTrustStores())
              .withTopic(configuration.getTopic())
              .withBootstrapServers(configuration.getBootstrapServers());
      Integer maxReadTimeSeconds = configuration.getMaxReadTimeSeconds();
      if (maxReadTimeSeconds != null) {
        kafkaRead = kafkaRead.withMaxReadTime(Duration.standardSeconds(maxReadTimeSeconds));
      }

      PCollection<byte[]> kafkaValues =
          input.getPipeline().apply(kafkaRead.withoutMetadata()).apply(Values.create());

      Schema errorSchema = ErrorHandling.errorSchemaBytes();
      PCollectionTuple outputTuple =
          kafkaValues.apply(
              ParDo.of(
                      new ErrorFn(
                          "Kafka-read-error-counter", valueMapper, errorSchema, handleErrors))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollectionRowTuple outputRows =
          PCollectionRowTuple.of("output", outputTuple.get(OUTPUT_TAG).setRowSchema(beamSchema));

      PCollection<Row> errorOutput = outputTuple.get(ERROR_TAG).setRowSchema(errorSchema);
      if (handleErrors) {
        outputRows =
            outputRows.and(
                checkArgumentNotNull(configuration.getErrorHandling()).getOutput(), errorOutput);
      }
      return outputRows;
    }
  }

  public static class ErrorFn extends DoFn<byte[], Row> {
    private final SerializableFunction<byte[], Row> valueMapper;
    private final Counter errorCounter;
    private Long errorsInBundle = 0L;
    private final boolean handleErrors;
    private final Schema errorSchema;

    public ErrorFn(
        String name,
        SerializableFunction<byte[], Row> valueMapper,
        Schema errorSchema,
        boolean handleErrors) {
      this.errorCounter = Metrics.counter(KafkaReadSchemaTransformProvider.class, name);
      this.valueMapper = valueMapper;
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void process(@DoFn.Element byte[] msg, MultiOutputReceiver receiver) {
      Row mappedRow = null;
      try {
        mappedRow = valueMapper.apply(msg);
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorsInBundle += 1;
        LOG.warn("Error while parsing the element", e);
        receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, msg, e));
      }
      if (mappedRow != null) {
        receiver.get(OUTPUT_TAG).output(mappedRow);
      }
    }

    @FinishBundle
    public void finish(FinishBundleContext c) {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  private static class ConsumerFactoryWithGcsTrustStores
      implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {

    @Override
    public Consumer<byte[], byte[]> apply(Map<String, Object> input) {
      return KafkaIOUtils.KAFKA_CONSUMER_FACTORY_FN.apply(
          input.entrySet().stream()
              .map(
                  entry ->
                      Maps.immutableEntry(
                          entry.getKey(), identityOrGcsToLocalFile(entry.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static Object identityOrGcsToLocalFile(Object configValue) {
      if (configValue instanceof String) {
        String configStr = (String) configValue;
        if (configStr.startsWith("gs://")) {
          try {
            Path localFile = Files.createTempFile("", "");
            LOG.info(
                "Downloading {} into local filesystem ({})", configStr, localFile.toAbsolutePath());
            // TODO(pabloem): Only copy if file does not exist.
            ReadableByteChannel channel =
                FileSystems.open(FileSystems.match(configStr).metadata().get(0).resourceId());
            FileOutputStream outputStream = new FileOutputStream(localFile.toFile());

            // Create a WritableByteChannel to write data to the FileOutputStream
            WritableByteChannel outputChannel = Channels.newChannel(outputStream);

            // Read data from the ReadableByteChannel and write it to the WritableByteChannel
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            while (channel.read(buffer) != -1) {
              buffer.flip();
              outputChannel.write(buffer);
              buffer.compact();
            }

            // Close the channels and the output stream
            channel.close();
            outputChannel.close();
            outputStream.close();
            return localFile.toAbsolutePath().toString();
          } catch (IOException e) {
            throw new IllegalArgumentException(
                String.format(
                    "Unable to fetch file %s to be used locally to create a Kafka Consumer.",
                    configStr));
          }
        } else {
          return configValue;
        }
      } else {
        return configValue;
      }
    }
  }
}
