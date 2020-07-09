package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.io.Serializable;

public enum CEPTypeName implements Serializable {
   BYTE,
   INT16,
   INT32,
   INT64,
   DECIMAL,
   FLOAT,
   DOUBLE,
   STRING,
   DATETIME,
   BOOLEAN
}
