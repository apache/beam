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

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.RenderResult;
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.spd.Relation;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;

@SuppressWarnings({"unchecked"})
public class MacroContext {

  Jinjava parser;

  public static String configuration(String... args) {
    return "";
  }

  public static String envVar(String arg) throws Exception {
    String env = System.getenv(arg);
    if (env == null) {
      throw new Exception("Environment variable '" + arg + "' is not set.");
    }
    return env;
  }

  public static String var(String... args) {
    return "";
  }

  public MacroContext() {
    parser = new Jinjava();
    // Register built-in functions
    parser.registerFunction(
        new ELFunctionDefinition(
            "", "config", MacroContext.class, "configuration", String[].class));
    parser.registerFunction(
        new ELFunctionDefinition("", "env_var", MacroContext.class, "envVar", String.class));
    // Register graph manipulation functions
    parser.registerFunction(
        new ELFunctionDefinition(
            "", "ref", GraphFunctions.class, "tableReference", String[].class));
    parser.registerFunction(
        new ELFunctionDefinition(
            "", "source", GraphFunctions.class, "sourceReference", String.class, String.class));
  }

  public RenderResult eval(String expr, Map<String, ?> binding) {
    return parser.renderForResult(expr, binding);
  }

  public RenderResult eval(String expr, StructuredPipelineDescription spd, List<Relation> tables) {
    Map<String, Object> binding = new HashMap<>();
    binding.put("_spd", spd);
    binding.put("_rel", tables);
    return eval(expr, binding);
  }

  public RenderResult eval(String expr, StructuredPipelineDescription spd) {
    return eval(expr, spd, new ArrayList<>());
  }
}
