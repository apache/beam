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

package org.apache.beam.model.pipeline;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.load.configuration.LoadingConfiguration;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

@RunWith(JUnit4.class)
public class PipelineTest {

  private static final String SCHEMA_URI = "resource:/org/apache/beam/model/pipeline/pipeline.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonSchema getSchema() throws ProcessingException {
    return JsonSchemaFactory.newBuilder()
            .setLoadingConfiguration(
                LoadingConfiguration.newBuilder()
                    .addParserFeature(JsonParser.Feature.ALLOW_COMMENTS)
                    .freeze())
            .freeze()
            .getJsonSchema(SCHEMA_URI);
  }

  @Test
  public void testFails() throws Exception {
    ProcessingReport report = getSchema().validate(MAPPER.readTree("{ \"ziggle\": 3 }"));
    assertFalse(report.isSuccess());
  }

  @Test
  public void testPasses() throws Exception {
    ProcessingReport report = getSchema().validate(MAPPER.readTree(
        "{ \"userFns\": {}, \"windowingStrategies\": {}, \"coders\": {}, \"transforms\": {},"
        + "\"values\": {} }"));

    if (!report.isSuccess()) {
      throw new ProcessingException(report.iterator().next());
    }
  }
}