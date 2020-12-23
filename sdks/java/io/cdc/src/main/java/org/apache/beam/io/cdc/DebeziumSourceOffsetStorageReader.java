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
package org.apache.beam.io.cdc;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DebeziumSourceOffsetStorageReader implements OffsetStorageReader {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumSourceOffsetStorageReader.class);
    private final Map<String, ?> offset;

    DebeziumSourceOffsetStorageReader(Map<String, ?> initialOffset) {
        this.offset = initialOffset;
    }

    @Override
    public <V> Map<String, Object> offset(Map<String, V> partition) {
        return offsets(Collections.singletonList(partition)).getOrDefault(partition, ImmutableMap.of());
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
        LOG.debug("-------------- GETTING OFFSETS!");

        Map<Map<String, T>, Map<String, Object>> map = new HashMap<>();
        for (Map<String, T> partition : partitions) {
            map.put(partition, (Map<String, Object>) offset);
        }

        LOG.debug("-------------- OFFSETS: {}", map);
        return map;
    }
}
