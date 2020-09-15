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
import com.google.cloud.datacatalog.v1beta1.Entry;
import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.Table.Builder;

/** {@link TableFactory} that understands Data Catalog Pubsub entries. */
class PubsubTableFactory implements TableFactory {

  private static final String PUBSUB_API = "pubsub.googleapis.com";

  private static final Pattern PS_PATH_PATTERN =
      Pattern.compile("/projects/(?<PROJECT>[^/]+)/topics/(?<TOPIC>[^/]+)");

  @Override
  public Optional<Builder> tableBuilder(Entry entry) {
    if (!URI.create(entry.getLinkedResource()).getAuthority().equalsIgnoreCase(PUBSUB_API)) {
      return Optional.empty();
    }

    return Optional.of(
        Table.builder()
            .location(getLocation(entry))
            .properties(new JSONObject())
            .type("pubsub")
            .comment(""));
  }

  private static String getLocation(Entry entry) {
    URI entryName = URI.create(entry.getLinkedResource());
    String psPath = entryName.getPath();

    Matcher bqPathMatcher = PS_PATH_PATTERN.matcher(psPath);
    if (!bqPathMatcher.matches()) {
      throw new IllegalArgumentException(
          "Unsupported format for Pubsub topic: '" + entry.getLinkedResource() + "'");
    }

    return psPath.substring(1);
  }
}
