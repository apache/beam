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
package org.apache.beam.sdk.io.kinesis.source;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kinesis.client.KinesisClientProvider;
import org.apache.beam.sdk.io.kinesis.client.SimplifiedKinesisClient;
import org.apache.beam.sdk.io.kinesis.client.response.KinesisRecord;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.StartingPoint;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.generator.CheckpointGenerator;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.generator.DynamicCheckpointGenerator;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.generator.StaticCheckpointGenerator;
import org.apache.beam.sdk.options.PipelineOptions;
import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.api.client.util.Lists.newArrayList;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;


/***
 *
 */
public class KinesisSource extends UnboundedSource<KinesisRecord, KinesisReaderCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

    private final KinesisClientProvider kinesis;
    private CheckpointGenerator initialCheckpointGenerator;

    public KinesisSource(KinesisClientProvider kinesis, String streamName,
                         StartingPoint startingPoint) {
        this(kinesis, new DynamicCheckpointGenerator(streamName, startingPoint));
    }

    private KinesisSource(KinesisClientProvider kinesisClientProvider,
                          CheckpointGenerator initialCheckpoint) {
        this.kinesis = kinesisClientProvider;
        this.initialCheckpointGenerator = initialCheckpoint;
        validate();
    }

    @Override
    public List<KinesisSource> generateInitialSplits(int desiredNumSplits,
                                                     PipelineOptions options) throws Exception {
        KinesisReaderCheckpoint checkpoint =
                initialCheckpointGenerator.generate(SimplifiedKinesisClient.from(kinesis));

        List<KinesisSource> sources = newArrayList();

        for (KinesisReaderCheckpoint partition : checkpoint.splitInto(desiredNumSplits)) {
            sources.add(new KinesisSource(
                    kinesis,
                    new StaticCheckpointGenerator(partition)));
        }
        return sources;
    }

    @Override
    public UnboundedReader<KinesisRecord> createReader(PipelineOptions options,
                                                KinesisReaderCheckpoint checkpointMark) {

        CheckpointGenerator checkpointGenerator = initialCheckpointGenerator;

        if (checkpointMark != null) {
            checkpointGenerator = new StaticCheckpointGenerator(checkpointMark);
        }

        LOG.info("Creating new reader using {}", checkpointGenerator);

        return new KinesisReader(
                SimplifiedKinesisClient.from(kinesis),
                checkpointGenerator,
                this);
    }

    @Override
    public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
        return SerializableCoder.of(KinesisReaderCheckpoint.class);
    }

    @Override
    public void validate() {
        checkNotNull(kinesis);
        checkNotNull(initialCheckpointGenerator);
    }

    @Override
    public Coder<KinesisRecord> getDefaultOutputCoder() {
        return KinesisRecordCoder.of();
    }
}
