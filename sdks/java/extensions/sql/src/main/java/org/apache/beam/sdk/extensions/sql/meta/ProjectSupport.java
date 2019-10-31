package org.apache.beam.sdk.extensions.sql.meta;

public enum ProjectSupport {
  NONE,
  WITHOUT_FIELD_REORDERING,
  WITH_FIELD_REORDERING;

  public boolean isProjectSupported() {
    return !this.equals(NONE);
  }

  public boolean isFieldReorderingSupported() {
    return this.equals(WITH_FIELD_REORDERING);
  }
}
