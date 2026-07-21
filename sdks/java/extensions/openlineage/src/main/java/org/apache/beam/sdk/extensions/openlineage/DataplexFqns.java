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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;

/**
 * Converts the Dataplex-style FQN strings reported through {@link
 * org.apache.beam.sdk.metrics.Lineage} by Beam IO connectors (e.g. {@code
 * pubsub:topic:project.topic}, {@code kafka:`host:9092`.topic}, {@code bigquery:p.d.t}) into {@link
 * DatasetIdentifier}s that follow the <a
 * href="https://openlineage.io/docs/spec/naming/">OpenLineage dataset naming conventions</a>.
 */
class DataplexFqns {

  /** Beam JDBC subprotocols whose OpenLineage spec scheme differs. */
  private static final Map<String, String> SCHEME_ALIASES = new HashMap<>();

  static {
    SCHEME_ALIASES.put("postgresql", "postgres");
    SCHEME_ALIASES.put("sqlserver", "mssql");
  }

  private DataplexFqns() {}

  /** Parses a flat Beam lineage FQN into an OpenLineage dataset identity. */
  static DatasetIdentifier toDatasetIdentifier(String fqn) {
    ParsedFqn parsed = ParsedFqn.parse(fqn);
    List<String> seg = parsed.segments;
    switch (parsed.system) {
      case "pubsub":
        {
          String subtype = parsed.subtype == null ? "topic" : parsed.subtype;
          return new DatasetIdentifier(subtype + ":" + String.join(":", seg), "pubsub");
        }
      case "kafka":
        {
          // Beam reports comma-joined bootstrap servers; the spec namespace is one host:port,
          // matching the Spark integration's KafkaBootstrapServerResolver (first server).
          String bootstrap = Splitter.on(',').splitToList(first(seg)).get(0);
          return new DatasetIdentifier(
              seg.size() > 1 ? seg.get(1) : "unknown", "kafka://" + bootstrap);
        }
      case "bigquery":
        return new DatasetIdentifier(String.join(".", seg), "bigquery");
      case "gcs":
        return new DatasetIdentifier(objectKey(seg, 1), "gs://" + first(seg));
      case "s3":
        return new DatasetIdentifier(objectKey(seg, 1), "s3://" + first(seg));
      case "abs":
        {
          // Beam segments: account, container, blob.
          String account = first(seg);
          String container = seg.size() > 1 ? seg.get(1) : "unknown";
          return new DatasetIdentifier(
              objectKey(seg, 2), "wasbs://" + container + "@" + account + ".dfs.core.windows.net");
        }
      case "hdfs":
        return new DatasetIdentifier(objectKey(seg, 1), "hdfs://" + first(seg));
      case "spanner":
        {
          // Beam segments: project, instanceConfig, instance, database, table.
          String project = first(seg);
          String instance = seg.size() > 2 ? seg.get(2) : "unknown";
          String database = seg.size() > 3 ? seg.get(3) : "unknown";
          String name = seg.size() > 4 ? database + "." + seg.get(4) : database;
          return new DatasetIdentifier(name, "spanner://" + project + ":" + instance);
        }
      default:
        {
          String scheme = SCHEME_ALIASES.getOrDefault(parsed.system, parsed.system);
          // JDBC-style FQNs carry host:port as the first segment.
          if (seg.size() >= 2 && seg.get(0).contains(":")) {
            return new DatasetIdentifier(
                String.join(".", seg.subList(1, seg.size())), scheme + "://" + seg.get(0));
          }
          return new DatasetIdentifier(String.join(".", seg), scheme);
        }
    }
  }

  /** Object-store names in the spec are the bare object key or path, without a leading slash. */
  private static String objectKey(List<String> seg, int fromIndex) {
    if (seg.size() <= fromIndex) {
      return "/";
    }
    String key = String.join("/", seg.subList(fromIndex, seg.size()));
    return key.startsWith("/") ? key.substring(1) : key;
  }

  private static String first(List<String> seg) {
    return seg.isEmpty() ? "unknown" : seg.get(0);
  }

  /**
   * Grammar of a Beam lineage FQN: {@code system ':' (subtype ':')? segment ('.' segment)*}. A
   * segment containing reserved characters is wrapped in backticks with literal backticks doubled
   * (see {@link org.apache.beam.sdk.metrics.Lineage#wrapSegment}).
   */
  static final class ParsedFqn {
    final String system;
    final @org.checkerframework.checker.nullness.qual.Nullable String subtype;
    final List<String> segments;

    private ParsedFqn(
        String system,
        @org.checkerframework.checker.nullness.qual.Nullable String subtype,
        List<String> segments) {
      this.system = system;
      this.subtype = subtype;
      this.segments = segments;
    }

    static ParsedFqn parse(String fqn) {
      // BoundedTrie-based lineage marks truncated FQNs with a trailing '*'.
      if (fqn.endsWith("*")) {
        fqn = fqn.substring(0, fqn.length() - 1);
      }
      int firstColon = fqn.indexOf(':');
      if (firstColon < 0) {
        return new ParsedFqn(fqn, null, new ArrayList<>());
      }
      String system = fqn.substring(0, firstColon);
      String rest = fqn.substring(firstColon + 1);
      String subtype = null;
      // Only Pub/Sub FQNs carry a subtype qualifier (topic/subscription).
      if ("pubsub".equals(system)) {
        int nextColon = rest.indexOf(':');
        if (nextColon > 0) {
          String candidate = rest.substring(0, nextColon);
          if (!candidate.contains(".") && !candidate.contains("`")) {
            subtype = candidate;
            rest = rest.substring(nextColon + 1);
          }
        }
      }
      return new ParsedFqn(system, subtype, splitSegments(rest));
    }

    /** Splits on unescaped '.', honoring backtick quoting with doubled-backtick escapes. */
    private static List<String> splitSegments(String s) {
      List<String> segments = new ArrayList<>();
      StringBuilder current = new StringBuilder();
      boolean inQuotes = false;
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (c == '`') {
          if (inQuotes && i + 1 < s.length() && s.charAt(i + 1) == '`') {
            current.append('`');
            i++;
          } else {
            inQuotes = !inQuotes;
          }
        } else if (c == '.' && !inQuotes) {
          segments.add(current.toString());
          current.setLength(0);
        } else {
          current.append(c);
        }
      }
      if (current.length() > 0) {
        segments.add(current.toString());
      }
      return segments;
    }
  }
}
