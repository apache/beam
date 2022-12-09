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
package org.apache.beam.sdk.extensions.spd.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.extensions.spd.NodeHandler;
import org.apache.beam.sdk.extensions.spd.SchemaTransformFactory;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class PTransformHandler implements NodeHandler, SchemaTransformFactory {
  private String name;
  private Schema config;

  public PTransformHandler(String name,Schema config) {
    this.name = name;
    this.config = config;
  }

  @Override
  public String tagName() {
    return name;
  }

  @Override
  public void internalVisit(JsonNode node, StructuredPipelineDescription description)
      throws Exception {

  }

  @Override
  public Schema getConfigurationSchema() {
    return config;
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<Row>> createTransform(Row config) {
    return null;
  }
}
