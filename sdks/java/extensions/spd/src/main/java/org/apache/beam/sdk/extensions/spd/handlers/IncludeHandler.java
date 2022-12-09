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
import java.io.InputStream;
import org.apache.beam.sdk.extensions.spd.NodeHandler;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;

public class IncludeHandler implements NodeHandler {

  @Override
  public String tagName() {
    return "include";
  }

  @Override
  public void internalVisit(JsonNode node, StructuredPipelineDescription description)
      throws Exception {
    String filename = node.asText();
    InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream(filename);
    if (in != null) {
      System.out.println("Including file '" + filename + "'");
      description.addValues(in);
      in.close();
    } else {
      throw new Exception("Unable to include '" + filename + "'");
    }
  }
}
