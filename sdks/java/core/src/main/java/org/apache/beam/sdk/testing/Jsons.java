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
package org.apache.beam.sdk.testing;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * Indirection for jackson in the test pipeline handler.
 */
public final class Jsons {
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModules(
        ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

    private Jsons() {
        // no-op
    }

    public static String[] toArgs(final PipelineOptions options) {
        try {
            byte[] opts = MAPPER.writeValueAsBytes(options);

            JsonParser jsonParser = MAPPER.getFactory().createParser(opts);
            TreeNode node = jsonParser.readValueAsTree();
            ObjectNode optsNode = (ObjectNode) node.get("options");
            ArrayList<String> optArrayList = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> entries = optsNode.fields();
            while (entries.hasNext()) {
                Map.Entry<String, JsonNode> entry = entries.next();
                if (entry.getValue().isNull()) {
                    continue;
                } else if (entry.getValue().isTextual()) {
                    optArrayList.add("--" + entry.getKey() + "=" + entry.getValue().asText());
                } else {
                    optArrayList.add("--" + entry.getKey() + "=" + entry.getValue());
                }
            }
            return optArrayList.toArray(new String[optArrayList.size()]);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String[] readOptions(final String beamTestPipelineOptions) {
        try {
            return MAPPER.readValue(beamTestPipelineOptions, String[].class);
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static PipelineOptions convertOptions(final PipelineOptions options) {
        return MAPPER.convertValue(MAPPER.valueToTree(options), PipelineOptions.class);
    }
}
