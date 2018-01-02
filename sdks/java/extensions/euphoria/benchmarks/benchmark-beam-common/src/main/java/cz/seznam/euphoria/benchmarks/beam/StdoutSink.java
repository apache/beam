/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.benchmarks.beam;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.options.PipelineOptions;

public class StdoutSink<T> extends Sink<T> {
    public static class StdoutWriteOperation<T> extends WriteOperation<T, Void> {

        public static class StdoutWriter<T> extends Writer<T, Void> {
            private final WriteOperation<T, Void> writeOperation;

            public StdoutWriter(WriteOperation<T, Void> writeOperation) {
                this.writeOperation = writeOperation;
            }

            @Override
            public void open(String uId) throws Exception {}

            @Override
            public void write(T value) throws Exception {
                System.out.println(value);
            }

            @Override
            public Void close() throws Exception {
                return null;
            }

            @Override
            public WriteOperation<T, Void> getWriteOperation() {
                return writeOperation;
            }
        }

        private final Sink<T> sink;

        public StdoutWriteOperation(Sink<T> sink) {
            this.sink = sink;
        }

        @Override
        public void initialize(PipelineOptions options) throws Exception {}

        @Override
        public void finalize(Iterable<Void> writerResults, PipelineOptions options) throws Exception {}

        @Override
        public Writer<T, Void> createWriter(PipelineOptions options) throws Exception {
            return new StdoutWriter<>(this);
        }

        @Override
        public Sink<T> getSink() {
            return sink;
        }

        @Override
        public Coder<Void> getWriterResultCoder() {
            return VoidCoder.of();
        }
    }

    @Override
    public void validate(PipelineOptions options) {}

    @Override
    public WriteOperation<T, ?> createWriteOperation(PipelineOptions options) {
        return new StdoutWriteOperation<>(this);
    }
}
