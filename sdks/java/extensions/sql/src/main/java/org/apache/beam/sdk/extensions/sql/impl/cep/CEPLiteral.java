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
package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.math.BigDecimal;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.joda.time.ReadableDateTime;

/**
 * {@code CEPLiteral} represents a literal node. It corresponds to {@code RexLiteral} in Calcite.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class CEPLiteral extends CEPOperation {

  private final Schema.TypeName typeName;

  private CEPLiteral(Schema.TypeName typeName) {
    this.typeName = typeName;
  }

  // TODO: deal with other types (byte, short...)
  public static CEPLiteral of(RexLiteral lit) {
    switch (lit.getTypeName()) {
      case INTEGER:
        return of(lit.getValueAs(Integer.class));
      case BIGINT:
        return of(lit.getValueAs(Long.class));
      case DECIMAL:
        return of(lit.getValueAs(BigDecimal.class));
      case FLOAT:
        return of(lit.getValueAs(Float.class));
      case DOUBLE:
        return of(lit.getValueAs(Double.class));
      case BOOLEAN:
        return of(lit.getValueAs(Boolean.class));
      case DATE:
        return of(lit.getValueAs(ReadableDateTime.class));
      case CHAR:
      case VARCHAR:
        return of(lit.getValueAs(String.class));
      default:
        throw new SqlConversionException("SQL type not supported: " + lit.getTypeName().toString());
    }
  }

  public static CEPLiteral of(Byte myByte) {
    return new CEPLiteral(Schema.TypeName.BYTE) {
      @Override
      public Byte getByte() {
        return myByte;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myByte.compareTo(otherLit.getByte());
      }
    };
  }

  public static CEPLiteral of(Short myShort) {
    return new CEPLiteral(Schema.TypeName.INT16) {
      @Override
      public Short getInt16() {
        return myShort;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myShort.compareTo(otherLit.getInt16());
      }
    };
  }

  public static CEPLiteral of(Integer myInt) {
    return new CEPLiteral(Schema.TypeName.INT32) {
      @Override
      public Integer getInt32() {
        return myInt;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myInt.compareTo(otherLit.getInt32());
      }
    };
  }

  public static CEPLiteral of(Long myLong) {
    return new CEPLiteral(Schema.TypeName.INT64) {
      @Override
      public Long getInt64() {
        return myLong;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myLong.compareTo(otherLit.getInt64());
      }
    };
  }

  public static CEPLiteral of(BigDecimal myDecimal) {
    return new CEPLiteral(Schema.TypeName.DECIMAL) {
      @Override
      public BigDecimal getDecimal() {
        return myDecimal;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myDecimal.compareTo(otherLit.getDecimal());
      }
    };
  }

  public static CEPLiteral of(Float myFloat) {
    return new CEPLiteral(Schema.TypeName.FLOAT) {
      @Override
      public Float getFloat() {
        return myFloat;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myFloat.compareTo(otherLit.getFloat());
      }
    };
  }

  public static CEPLiteral of(Double myDouble) {
    return new CEPLiteral(Schema.TypeName.DOUBLE) {
      @Override
      public Double getDouble() {
        return myDouble;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myDouble.compareTo(otherLit.getDouble());
      }
    };
  }

  public static CEPLiteral of(ReadableDateTime myDateTime) {
    return new CEPLiteral(Schema.TypeName.DATETIME) {
      @Override
      public ReadableDateTime getDateTime() {
        return myDateTime;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myDateTime.compareTo(otherLit.getDateTime());
      }
    };
  }

  public static CEPLiteral of(Boolean myBoolean) {
    return new CEPLiteral(Schema.TypeName.BOOLEAN) {
      @Override
      public Boolean getBoolean() {
        return myBoolean;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myBoolean.compareTo(otherLit.getBoolean());
      }
    };
  }

  public static CEPLiteral of(String myString) {
    return new CEPLiteral(Schema.TypeName.STRING) {
      @Override
      public String getString() {
        return myString;
      }

      @Override
      public int compareTo(Object other) {
        if (!(other instanceof CEPLiteral)) {
          throw new IllegalStateException("The other object should be an instance of CEPLiteral");
        }
        CEPLiteral otherLit = (CEPLiteral) other;
        if (getTypeName() != otherLit.getTypeName()) {
          throw new IllegalStateException(
              "The other CEPLiteral should have type "
                  + getTypeName().toString()
                  + ", given: "
                  + otherLit.getTypeName().toString());
        }
        return myString.compareTo(otherLit.getString());
      }
    };
  }

  public int compareTo(Object other) {
    throw new IllegalStateException("the class must be subclassed properly to use this method");
  };

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CEPLiteral)) {
      throw new IllegalStateException("The other object should be an instance of CEPLiteral");
    }
    return this.compareTo(other) == 0;
  }

  @Override
  public int hashCode() {
    return typeName.hashCode();
  }

  public Byte getByte() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public Short getInt16() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public Integer getInt32() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public Long getInt64() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public BigDecimal getDecimal() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public Float getFloat() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public Double getDouble() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public ReadableDateTime getDateTime() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public Boolean getBoolean() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public String getString() {
    throw new IllegalStateException("the class must be subclassed properly to get the value");
  }

  public Schema.TypeName getTypeName() {
    return typeName;
  }
}
