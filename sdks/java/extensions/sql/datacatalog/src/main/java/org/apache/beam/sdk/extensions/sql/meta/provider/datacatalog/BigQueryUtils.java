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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import com.alibaba.fastjson.JSONObject;
import com.google.cloud.datacatalog.Entry;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/** Utils to extract BQ-specific entry information. */
class BigQueryUtils {

  private static final Pattern BQ_PATH_PATTERN =
      Pattern.compile(
          "/projects/(?<PROJECT>[^/]+)/datasets/(?<DATASET>[^/]+)/tables/(?<TABLE>[^/]+)");

  static Table.Builder tableBuilder(Entry entry) {
    return Table.builder()
        .location(getLocation(entry))
        .properties(new JSONObject())
        .type("bigquery")
        .comment("");
  }

  private static String getLocation(Entry entry) {
    URI entryName = URI.create(entry.getLinkedResource());
    String bqPath = entryName.getPath();

    Matcher bqPathMatcher = BQ_PATH_PATTERN.matcher(bqPath);
    if (!bqPathMatcher.matches()) {
      throw new IllegalArgumentException(
          "Unsupported format for BigQuery table path: '" + entry.getLinkedResource() + "'");
    }

    String project = bqPathMatcher.group("PROJECT");
    String dataset = bqPathMatcher.group("DATASET");
    String table = bqPathMatcher.group("TABLE");

    return String.format("%s:%s.%s", project, dataset, table);
  }
}
