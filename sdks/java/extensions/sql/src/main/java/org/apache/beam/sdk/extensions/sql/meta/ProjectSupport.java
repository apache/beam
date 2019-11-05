package org.apache.beam.sdk.extensions.sql.meta;

public enum ProjectSupport {
  NONE,
  WITHOUT_FIELD_REORDERING,
  WITH_FIELD_REORDERING;

  public boolean isSupported() {
    return !this.equals(NONE);
  }

  public boolean withFieldReordering() {
    return this.equals(WITH_FIELD_REORDERING);
  }
}
