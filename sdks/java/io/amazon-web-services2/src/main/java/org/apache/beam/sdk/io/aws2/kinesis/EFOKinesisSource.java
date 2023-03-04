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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

class EFOKinesisSource extends UnboundedSource<KinesisRecord, KinesisReaderCheckpoint> {

  private final KinesisIO.Read readSpec;
  private final EFOCheckpointGenerator checkpointGenerator;
  private final ClientBuilderFactory builderFactory;

  EFOKinesisSource(KinesisIO.Read readSpec, ClientBuilderFactory builderFactory) {
    this(readSpec, builderFactory, new EFOFromScratchCheckpointGenerator(readSpec));
  }

  private EFOKinesisSource(
      KinesisIO.Read readSpec,
      ClientBuilderFactory builderFactory,
      EFOCheckpointGenerator initialCheckpoint) {
    this.readSpec = checkArgumentNotNull(readSpec);
    this.builderFactory = builderFactory;
    this.checkpointGenerator = checkArgumentNotNull(initialCheckpoint);
  }

  @Override
  public List<EFOKinesisSource> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {
    try (KinesisAsyncClient kinesis = createClient()) {
      KinesisReaderCheckpoint checkpoint = checkpointGenerator.generate(kinesis);
      List<EFOKinesisSource> sources = newArrayList();
      for (KinesisReaderCheckpoint partition : checkpoint.splitInto(desiredNumSplits)) {
        sources.add(
            new EFOKinesisSource(
                readSpec, builderFactory, new EFOStaticCheckpointGenerator(partition)));
      }
      return sources;
    }
  }

  @Override
  public UnboundedReader<KinesisRecord> createReader(
      PipelineOptions options, @Nullable KinesisReaderCheckpoint checkpointMark)
      throws IOException {
    EFOCheckpointGenerator checkpointGenerator = this.checkpointGenerator;
    if (checkpointMark != null) {
      checkpointGenerator = new EFOStaticCheckpointGenerator(checkpointMark);
    }

    EFOKinesisSource source = new EFOKinesisSource(readSpec, builderFactory);
    return new EFOKinesisReader(readSpec, createClient(), checkpointGenerator, source);
  }

  @Override
  public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(KinesisReaderCheckpoint.class);
  }

  @Override
  public Coder<KinesisRecord> getOutputCoder() {
    return KinesisRecordCoder.of();
  }

  private KinesisAsyncClient createClient() {
    return builderFactory
        .create(
            KinesisClientUtil.adjustKinesisClientBuilder(KinesisAsyncClient.builder()),
            checkArgumentNotNull(readSpec.getClientConfiguration()),
            null) // builderFactory already created with AwsOptions
        .build();
  }
}
