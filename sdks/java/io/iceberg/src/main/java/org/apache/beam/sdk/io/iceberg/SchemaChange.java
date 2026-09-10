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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * One change that registering a file schema would make on the table, classified by the {@link
 * SchemaEvolutionOption} it needs. Produced by {@link SchemaDelta#classify} (and, for name
 * conflicts, {@link ColumnNameChecks}); {@link SchemaDelta} decides whether a file's set of changes
 * is allowed by a {@link SchemaEvolutionConfig}.
 */
final class SchemaChange {

  /** The option a change needs to be allowed. */
  enum Kind {
    FIELD_ADDITION(SchemaEvolutionOption.ALLOW_FIELD_ADDITION),
    FIELD_RELAXATION(SchemaEvolutionOption.ALLOW_FIELD_RELAXATION),
    TYPE_PROMOTION(SchemaEvolutionOption.ALLOW_TYPE_PROMOTION),
    /** The union is impossible (for example string vs int); never allowed. */
    CONFLICT(null);

    final @Nullable SchemaEvolutionOption option;

    Kind(@Nullable SchemaEvolutionOption option) {
      this.option = option;
    }

    boolean allowedBy(SchemaEvolutionConfig config) {
      return option != null && config.allows(option);
    }
  }

  final Kind kind;

  /** Unquoted column path for the config lookup; empty for conflicts without a field. */
  final String path;

  final String description;

  /** A relaxation because the column is absent from the file, not declared optional. */
  final boolean absent;

  SchemaChange(Kind kind, String path, String description) {
    this(kind, path, description, false);
  }

  SchemaChange(Kind kind, String path, String description, boolean absent) {
    this.kind = kind;
    this.path = path;
    this.description = description;
    this.absent = absent;
  }

  boolean allowedBy(SchemaEvolutionConfig config, Pins pins) {
    // A pin also forbids relaxing the structs above it: a null ancestor nulls the pinned leaf.
    if (kind == Kind.FIELD_RELAXATION
        && (pins.isPinned(path) || pins.pinnedColumnBeneath(path) != null)) {
      return false;
    }
    return kind.allowedBy(config);
  }

  String disallowedReason(Pins pins) {
    if (kind == Kind.FIELD_RELAXATION) {
      if (pins.isPinned(path)) {
        return description + " (pinned as required)";
      }
      @Nullable String pin = pins.pinnedColumnBeneath(path);
      if (pin != null) {
        return description + " (ancestor of pinned column " + pin + ")";
      }
    }
    return description + " (needs " + kind.option + ")";
  }
}
