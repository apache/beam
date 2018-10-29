package org.apache.beam.sdk.extensions.sql.impl.udf;

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;

public class HyperbolicSine implements BeamSqlUdf {
  public static final String FUNCTION_NAME = "SINH";

  // TODO: handle overflow
  public static Double eval(Double o) {
    return o == null ? null : Math.sinh(o);
  }
}
