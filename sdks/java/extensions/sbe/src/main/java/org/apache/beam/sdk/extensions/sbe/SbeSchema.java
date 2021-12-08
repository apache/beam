package org.apache.beam.sdk.extensions.sbe;

import org.apache.beam.sdk.schemas.Schema;

public final class SbeSchema {

  // TODO(zhoufek): Add factory methods for stubs, IR, and XML

  public Schema schema() {
    Schema.Builder schema = Schema.builder();
    // TODO(zhoufek): Build
    return schema.build();
  }
}
