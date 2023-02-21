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
package org.apache.beam.sdk.io.aws2.kinesis;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        KinesisReadSchemaTransformProvider.KinesisReadSchemaTransformConfiguration> {
  public static final ImmutableSet<String> SUPPORTED_INITIAL_POSITIONS =
      ImmutableSet.of("LATEST", "TRIM_HORIZON", "AT_TIMESTAMP");

  public static final ImmutableSet<String> SUPPORTED_FORMATS = ImmutableSet.of("AVRO", "JSON");

  public static final TupleTag<Row> MAIN_TAG = new TupleTag<Row>("main");
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>("errors");
  public static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("error").addNullableByteArrayField("row").build();

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:kinesis_read:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Lists.newArrayList("output", "errors");
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<KinesisReadSchemaTransformConfiguration>
      configurationClass() {
    return KinesisReadSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      KinesisReadSchemaTransformConfiguration configuration) {
    if (!SUPPORTED_INITIAL_POSITIONS.contains(configuration.getInitialPositionInStream())) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown initial position in stream %s. Supported values: %s",
              configuration.getInitialPositionInStream(), SUPPORTED_INITIAL_POSITIONS));
    }
    if (!SUPPORTED_FORMATS.contains(configuration.getFormat().toUpperCase())) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown data format %s. Supported values: %s",
              configuration.getFormat(), SUPPORTED_FORMATS));
    }
    return new KinesisReadSchemaTransform(configuration);
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class KinesisReadSchemaTransformConfiguration implements Serializable {
    public abstract String getFormat();

    public abstract String getSchema();

    public abstract String getStreamName();

    @SchemaFieldDescription("Supported values are [\"LATEST\", \"TRIM_HORIZON\", \"AT_TIMESTAMP\"]")
    public abstract String getInitialPositionInStream();

    @SchemaFieldDescription(
        "Access key ID for AWS credentials. See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html")
    public abstract String getCredentialsAccessKeyId();

    @SchemaFieldDescription(
        "Secret access key for AWS credentials. See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html")
    public abstract String getCredentialsSecretAccessKey();

    public static Builder builder() {
      return new AutoValue_KinesisReadSchemaTransformProvider_KinesisReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFormat(String format);

      public abstract Builder setStreamName(String streamName);

      public abstract Builder setSchema(String schema);

      public abstract Builder setInitialPositionInStream(String initialPosition);

      public abstract Builder setCredentialsAccessKeyId(String accessKeyId);

      public abstract Builder setCredentialsSecretAccessKey(String secretAccessKey);

      public abstract KinesisReadSchemaTransformConfiguration build();
    }
  }

  static class KinesisReadSchemaTransform implements SchemaTransform {

    final KinesisReadSchemaTransformConfiguration configuration;

    KinesisReadSchemaTransform(KinesisReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PTransform<
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
        buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          final String inputSchema = configuration.getSchema();
          final Schema beamSchema =
              Objects.equals(configuration.getFormat(), "JSON")
                  ? JsonUtils.beamSchemaFromJsonSchema(inputSchema)
                  : AvroUtils.toBeamSchema(new org.apache.avro.Schema.Parser().parse(inputSchema));
          SerializableFunction<byte[], Row> valueMapper =
              Objects.equals(configuration.getFormat(), "JSON")
                  ? JsonUtils.getJsonBytesToRowFunction(beamSchema)
                  : AvroUtils.getAvroBytesToRowFunction(beamSchema);
          PCollectionTuple result =
              input
                  .getPipeline()
                  .apply(
                      KinesisIO.read()
                          .withStreamName(configuration.getStreamName())
                          .withInitialPositionInStream(
                              InitialPositionInStream.valueOf(
                                  configuration.getInitialPositionInStream()))
                          .withClientConfiguration(
                              ClientConfiguration.builder()
                                  .credentialsProvider(
                                      StaticCredentialsProvider.create(
                                          AwsBasicCredentials.create(
                                              configuration.getCredentialsAccessKeyId(),
                                              configuration.getCredentialsSecretAccessKey())))
                                  .build())
                          .withProcessingTimeWatermarkPolicy())
                  .apply(
                      ParDo.of(
                              new DoFn<KinesisRecord, Row>() {
                                @DoFn.ProcessElement
                                public void process(
                                    @DoFn.Element KinesisRecord record,
                                    MultiOutputReceiver receiver) {
                                  try {
                                    receiver
                                        .getRowReceiver(MAIN_TAG)
                                        .output(valueMapper.apply(record.getDataAsBytes()));
                                  } catch (Throwable e) {
                                    receiver
                                        .getRowReceiver(ERROR_TAG)
                                        .output(
                                            Row.withSchema(ERROR_SCHEMA)
                                                .addValues(e.toString(), record.getDataAsBytes())
                                                .build());
                                  }
                                }
                              })
                          .withOutputTags(MAIN_TAG, TupleTagList.of(ERROR_TAG)));
          return PCollectionRowTuple.of(
              "output",
              result.get(MAIN_TAG).setRowSchema(beamSchema),
              "errors",
              result.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA));
        }
      };
    }
  }
}
