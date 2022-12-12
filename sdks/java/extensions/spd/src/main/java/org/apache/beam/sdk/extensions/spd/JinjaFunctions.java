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

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition;
import org.apache.beam.sdk.extensions.sql.meta.Table;

public class JinjaFunctions {
  public static String reference(String... model) {
    JinjavaInterpreter interpreter = JinjavaInterpreter.getCurrent();

    StructuredPipelineDescription spdObject =
        (StructuredPipelineDescription) interpreter.getContext().get("_spd");
    Table table = spdObject.findTable(model[0]);
    if (table == null) {
      return "";
    } else {
      return table.getName();
    }
  }

  public static Jinjava getDefault() {
    Jinjava parser = new Jinjava();
    parser.registerFunction(
        new ELFunctionDefinition("", "ref", JinjaFunctions.class, "reference", String[].class));

    return parser;
  }
}
