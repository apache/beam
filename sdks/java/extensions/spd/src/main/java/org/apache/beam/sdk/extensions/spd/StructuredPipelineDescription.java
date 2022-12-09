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
package org.apache.beam.sdk.extensions.spd;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.beam.sdk.extensions.spd.handlers.IncludeHandler;
import org.apache.beam.sdk.extensions.spd.handlers.TransformHandler;

public class StructuredPipelineDescription {
  private static ObjectMapper DEFAULT_MAPPER = new ObjectMapper(new YAMLFactory());

  private ObjectMapper mapper;

  private ArrayList<ObjectNode> nodes;
  private HashMap<String, NodeHandler> handlers;

  private void processNodes() throws Exception {
    if (nodes.size() == 0) return;

    boolean nextCycle = false;

    do {
      ArrayList<ObjectNode> working = nodes;
      System.out.println("Processing " + working.size() + " nodes.");
      ArrayList<Exception> exceptions = new ArrayList<>();
      this.nodes = new ArrayList<ObjectNode>();
      int processed = 0;
      for (ObjectNode n : working) {
        boolean foundTag = false;
        for (HashMap.Entry<String, NodeHandler> e : handlers.entrySet()) {
          if (n.has(e.getKey())) {
            foundTag = true;
            try {
              e.getValue().visit(n.get(e.getKey()), this);
              processed++;
            } catch (Exception ex) {
              exceptions.add(ex);
            }
          }
          if (foundTag) break;
        }
        if (!foundTag) {
          Iterator<String> fieldNames = n.fieldNames();
          while (fieldNames.hasNext())
            exceptions.add(
                new Exception("Unable to find handle for tag '" + fieldNames.next() + "'"));
        }
      }

      if (processed == 0 && nodes.size() > 0) throw new StructuredPipelineException(exceptions);

      // If we have more nodes do another round
      nextCycle = nodes.size() > 0;

    } while (nextCycle);
  }

  public StructuredPipelineDescription(ObjectMapper mapper) {
    this.mapper = mapper;
    this.nodes = new ArrayList<>();
    this.handlers = new HashMap<String, NodeHandler>();
    this.handlers.put("include", new IncludeHandler());
    this.handlers.put("transform", new TransformHandler());
  }

  public StructuredPipelineDescription() {
    this(DEFAULT_MAPPER);
  }

  public void registerNodeHandler(NodeHandler handler) throws Exception {
    String tagName = handler.tagName();
    if (handlers.containsKey(tagName)) {
      throw new Exception("Attempting to redefine tag named '" + tagName + "'");
    }
    handlers.put(tagName, handler);
  }

  public void addValues(JsonParser parser) throws IOException {
    mapper
        .readValues(parser, ObjectNode.class)
        .forEachRemaining(
            (o) -> {
              nodes.add(o);
            });
  }

  public void addValues(InputStream in) throws IOException {
    addValues(mapper.createParser(in));
  }

  public void addValues(Reader reader) throws IOException {
    addValues(mapper.createParser(reader));
  }

  public void addValues(String values) throws IOException {
    addValues(mapper.createParser(values));
  }

  public void includeStdlib() throws Exception {
    InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream("stdlib.yaml");
    if (in != null) {
      addValues(mapper.createParser(in));
    }
    processNodes();
  }
}
