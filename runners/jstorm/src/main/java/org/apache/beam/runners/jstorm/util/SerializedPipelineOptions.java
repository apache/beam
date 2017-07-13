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

package org.apache.beam.runners.jstorm.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encapsulates the PipelineOptions in serialized form to ship them to the cluster.
 */
public class SerializedPipelineOptions implements Serializable {

    private final byte[] serializedOptions;

    /** Lazily initialized copy of deserialized options */
    private transient PipelineOptions pipelineOptions;

    public SerializedPipelineOptions(PipelineOptions options) {
        checkNotNull(options, "PipelineOptions must not be null.");

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            new ObjectMapper().writeValue(baos, options);
            this.serializedOptions = baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Couldn't serialize PipelineOptions.", e);
        }

    }

    public PipelineOptions getPipelineOptions() {
        if (pipelineOptions == null) {
            try {
                pipelineOptions = new ObjectMapper().readValue(serializedOptions, PipelineOptions.class);
            } catch (IOException e) {
                throw new RuntimeException("Couldn't deserialize the PipelineOptions.", e);
            }
        }

        return pipelineOptions;
    }

}
