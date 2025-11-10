package org.apache.beam.sdk.lineage;

import com.facebook.presto.hadoop.$internal.org.apache.avro.reflect.Nullable;

public interface LineageReporter {
  /**
   * Adds lineage information using pre-formatted FQN segments.
   *
   * @param rollupSegments FQN segments already escaped per Dataplex format
   */
  void add(Iterable<String> rollupSegments);

  /**
   * Adds lineage with system, optional subtype, and hierarchical segments.
   *
   * @param system The data system identifier (e.g., "bigquery", "kafka")
   * @param subtype Optional subtype (e.g., "table", "topic"), may be null
   * @param segments Hierarchical path segments
   * @param lastSegmentSep Separator for the last segment, may be null
   */
  void add(
      String system,
      @Nullable String subtype,
      Iterable<String> segments,
      @Nullable String lastSegmentSep);

  /**
   * Add a FQN (fully-qualified name) to Lineage.
   */
  default void add(String system, Iterable<String> segments, @Nullable String sep) {
    add(system, null, segments, sep);
  }

  /**
   * Add a FQN (fully-qualified name) to Lineage.
   */
  default void add(String system, Iterable<String> segments) {
    add(system, segments, null);
  }
}
