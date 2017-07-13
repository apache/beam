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
package org.apache.beam.runners.jstorm.translation.translator;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class that defines the stream connection between upstream and downstream components.
 */
@AutoValue
public abstract class Stream {

    public abstract Producer getProducer();
    public abstract Consumer getConsumer();

    public static Stream of(Producer producer, Consumer consumer) {
        return new com.alibaba.jstorm.beam.translation.translator.AutoValue_Stream(producer, consumer);
    }

    @AutoValue
    public abstract static class Producer {
        public abstract String getComponentId();
        public abstract String getStreamId();
        public abstract String getStreamName();

        public static Producer of(String componentId, String streamId, String streamName) {
            return new com.alibaba.jstorm.beam.translation.translator.AutoValue_Stream_Producer(
                    componentId, streamId, streamName);
        }
    }

    @AutoValue
    public abstract static class Consumer {
        public abstract String getComponentId();
        public abstract Grouping getGrouping();

        public static Consumer of(String componentId, Grouping grouping) {
            return new com.alibaba.jstorm.beam.translation.translator.AutoValue_Stream_Consumer(
                    componentId, grouping);
        }
    }

    @AutoValue
    public abstract static class Grouping {
        public abstract Type getType();

        @Nullable
        public abstract List<String> getFields();

        public static Grouping of(Type type) {
            checkArgument(!Type.FIELDS.equals(type), "Fields grouping should provide key fields.");
            return new com.alibaba.jstorm.beam.translation.translator.AutoValue_Stream_Grouping(
                    type, null /* fields */);
        }

        public static Grouping byFields(List<String> fields) {
            checkNotNull(fields, "fields");
            checkArgument(!fields.isEmpty(), "No key fields were provided for field grouping!");
            return new com.alibaba.jstorm.beam.translation.translator.AutoValue_Stream_Grouping(
                    Type.FIELDS, fields);
        }

        /**
         * Types of stream groupings Storm allows
         */
        public enum Type {
            ALL, CUSTOM, DIRECT, SHUFFLE, LOCAL_OR_SHUFFLE, FIELDS, GLOBAL, NONE
        }
    }
}
