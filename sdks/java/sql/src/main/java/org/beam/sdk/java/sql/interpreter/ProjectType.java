package org.beam.sdk.java.sql.interpreter;

import java.io.Serializable;

public enum ProjectType implements Serializable {
  RexLiteral, RexInputRef, RexCall;
}
