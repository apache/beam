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
package org.apache.beam.sdk.extensions.spd.profile.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.hubspot.jinjava.interpret.RenderResult;
import com.hubspot.jinjava.interpret.TemplateError;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;
import org.apache.beam.sdk.extensions.spd.macros.MacroContext;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(ProfileAdapter.class)
@SuppressWarnings({"nullness"})
public class MarkdownAdapter extends DefaultAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(MarkdownAdapter.class);

  private TestTableProvider provider = new TestTableProvider();

  @Override
  public String getName() {
    return "markdown";
  }

  @Override
  public void preLoadMaterializationHook(StructuredPipelineDescription pipelineDescription) {
    // We register the test table provider first for materialization
    pipelineDescription.registerProvider(provider);
  }

  @Override
  public void postExecuteMaterializationHook(StructuredPipelineDescription pipelineDescription) {
    super.postExecuteMaterializationHook(pipelineDescription);
    MacroContext macroContext = new MacroContext();
    Map<String, ?> binding = ImmutableMap.of("_tbl", provider);
    for (Path report : pipelineDescription.getReportFiles()) {
      if (report.getFileName().toString().endsWith(".md")) {
        try {
          RenderResult result =
              macroContext.eval(String.join("\n", Files.readAllLines(report)), binding);
          if (result.hasErrors()) {
            for (TemplateError templateError : result.getErrors()) {
              throw templateError.getException();
            }
          }
          Path p =
              pipelineDescription.getTargetPath(report.getFileName().toString()).toAbsolutePath();
          LOG.info("Writing to {}", p);
          Files.createDirectories(p.getParent());
          Files.write(p, ImmutableList.of(result.getOutput()));
        } catch (Exception e) {
          LOG.info("Got report exception: {}", e);
        }
      }
    }
    // Get markdown files and process
  }

  @Override
  public Table getSourceTable(JsonNode profile, Table table) {
    throw new UnsupportedOperationException(
        "`markdown` adapter type can only be used for materialization");
  }

  @Override
  public Table getMaterializedTable(JsonNode profile, Table table) {
    return super.getMaterializedTable(profile, table).toBuilder().type("test").build();
  }
}
