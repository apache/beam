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
package com.alibaba.jstorm.beam.example;

import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

public class RandomSentenceSource extends UnboundedSource<String, UnboundedSource.CheckpointMark> {
    private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSource.class);

    private final Coder<String> coder;

    public RandomSentenceSource(Coder<String> coder) {
        this.coder = coder;
    }

    @Override
    public List<? extends UnboundedSource<String, CheckpointMark>> split(int i, PipelineOptions pipelineOptions) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnboundedReader<String> createReader(PipelineOptions pipelineOptions, @Nullable CheckpointMark checkpointMark) throws IOException {
        return new RandomSentenceReader(this);
    }

    @Nullable
    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
        return null;
    }

    @Override
    public void validate() {

    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
        return this.coder;
    }

    public static class RandomSentenceReader extends UnboundedReader<String> {

        private String[] values = { "blah blah blah", "foo bar", "my dog has fleas" };
        private int index = 0;
        private final UnboundedSource<String, CheckpointMark> source;
        private final Random rand = new Random(System.currentTimeMillis());

        public RandomSentenceReader(UnboundedSource<String, CheckpointMark> source) {
            this.source = source;
        }

        @Override
        public boolean start() throws IOException {
            index = 0;
            LOG.info("start done");
            return true;
        }

        @Override
        public boolean advance() throws IOException {
            if (rand.nextInt() % values.length == 0) {
                JStormUtils.sleepMs(1000);
                LOG.info("advance done");
                return false;
            }
            index++;
            return true;
        }

        @Override
        public Instant getWatermark() {
            return Instant.now();
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return null;
        }

        @Override
        public UnboundedSource<String, ?> getCurrentSource() {
            return this.source;
        }

        @Override
        public String getCurrent() throws NoSuchElementException {
            return values[index % values.length];
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return Instant.now();
        }

        @Override
        public void close() throws IOException {

        }
    }
}
