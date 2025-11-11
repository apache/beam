///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.beam.sdk.lineage;
//
//import com.facebook.presto.hadoop.$internal.org.apache.avro.reflect.Nullable;
//
//public interface LineageReporter {
//  /**
//   * Adds lineage information using pre-formatted FQN segments.
//   *
//   * @param rollupSegments FQN segments already escaped per Dataplex format
//   */
//  void add(Iterable<String> rollupSegments);
//
//  /**
//   * Adds lineage with system, optional subtype, and hierarchical segments.
//   *
//   * @param system The data system identifier (e.g., "bigquery", "kafka")
//   * @param subtype Optional subtype (e.g., "table", "topic"), may be null
//   * @param segments Hierarchical path segments
//   * @param lastSegmentSep Separator for the last segment, may be null
//   */
//  void add(
//      String system,
//      @Nullable String subtype,
//      Iterable<String> segments,
//      @Nullable String lastSegmentSep);
//
//  /** Add a FQN (fully-qualified name) to Lineage. */
//  default void add(String system, Iterable<String> segments, @Nullable String sep) {
//    add(system, null, segments, sep);
//  }
//
//  /** Add a FQN (fully-qualified name) to Lineage. */
//  default void add(String system, Iterable<String> segments) {
//    add(system, segments, null);
//  }
//}
