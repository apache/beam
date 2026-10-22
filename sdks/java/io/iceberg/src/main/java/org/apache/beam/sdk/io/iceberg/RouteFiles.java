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
package org.apache.beam.sdk.io.iceberg;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Names the destination table for each file path, from the pattern that describes the layout and
 * the template that spells the identifier.
 *
 * <p>A path the pattern does not match is not something this transform was pointed at, so it
 * becomes an error row rather than being dropped: a mistyped pattern has to be visible, not
 * silently register nothing.
 */
final class RouteFiles extends DoFn<String, KV<String, String>> {

  static final TupleTag<KV<String, String>> ROUTED = new TupleTag<>();
  static final TupleTag<Row> ERRORS = new TupleTag<>();

  /** {@code table} is empty when nothing matched, since there is no table to attribute it to. */
  static final Schema ERROR_SCHEMA =
      Schema.builder()
          .addStringField("file")
          .addStringField("error")
          .addStringField("table")
          .build();

  private final String pathPattern;
  private final String tableTemplate;
  private final PathPattern pattern;

  RouteFiles(String pathPattern, String tableTemplate) {
    this.pathPattern = pathPattern;
    this.tableTemplate = tableTemplate;
    // both throw here, at pipeline construction, rather than once per file in a worker
    this.pattern = PathPattern.compile(pathPattern);
    this.pattern.checkRenderable(tableTemplate);
  }

  @ProcessElement
  public void process(@Element String path, MultiOutputReceiver output) {
    PathPattern.Match match = pattern.match(path);

    if (match == null) {
      String error = "did not match path_pattern '" + pathPattern + "'";
      output.get(ERRORS).output(errorRow(path, error, ""));
      return;
    }
    String table = PathPattern.render(tableTemplate, match.captures());
    output.get(ROUTED).output(KV.of(table, path));
  }

  private static Row errorRow(String file, String error, String table) {
    return Row.withSchema(ERROR_SCHEMA).addValues(file, error, table).build();
  }
}
