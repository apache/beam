package org.beam.sdk.java.sql.transform;

import java.io.Serializable;

public enum ProjectType implements Serializable {
  RexLiteral, RexInputRef, RexCall;
}
