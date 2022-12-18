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
package org.apache.beam.sdk.extensions.spd.macros;

import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import org.apache.beam.sdk.extensions.spd.Relation;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphFunctions {
  private static final Logger LOG = LoggerFactory.getLogger(GraphFunctions.class);

  public static Relation tableReference(String... args) throws Exception {
    StructuredPipelineDescription spd =
        (StructuredPipelineDescription) JinjavaInterpreter.getCurrent().getContext().get("_spd");
    //  String packageName = args.length == 1 ? "default" : args[0];
    String tableName = args.length == 1 ? args[0] : args[1];
    LOG.info("Trying to find relation " + tableName);
    return spd.getRelation(tableName);
  }

  public static Relation sourceReference(String sourceName, String tableName) throws Exception {
    StructuredPipelineDescription spd =
        (StructuredPipelineDescription) JinjavaInterpreter.getCurrent().getContext().get("_spd");
    return spd.getRelation(tableName);
  }
}
