package org.apache.beam.sdk.schemas.logicaltypes;

import org.apache.beam.sdk.schemas.Schema.FieldType;

/** A LogicalType corresponding to CHAR. */
public class CharType extends PassThroughLogicalType<String> {
    public static final String IDENTIFIER = "SqlCharType";

    public CharType() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.STRING);
    }
}