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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.joda.time.ReadableDateTime;

public class CEPLiteral extends CEPOperation {

  private final CEPTypeName typeName;

  private CEPLiteral(CEPTypeName typeName) {
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
        throw new SqlConversionException(
            "sql literal type not supported: " + lit.getTypeName().toString());
    }
  }

  public static CEPLiteral of(Byte myByte) {
    return new CEPLiteral(CEPTypeName.BYTE) {
      @Override
      public Byte getByte() {
        return myByte;
      }
    };
  }

  public static CEPLiteral of(Short myShort) {
    return new CEPLiteral(CEPTypeName.INT16) {
      @Override
      public Short getInt16() {
        return myShort;
      }
    };
  }

  public static CEPLiteral of(Integer myInt) {
    return new CEPLiteral(CEPTypeName.INT32) {
      @Override
      public Integer getInt32() {
        return myInt;
      }
    };
  }

  public static CEPLiteral of(Long myLong) {
    return new CEPLiteral(CEPTypeName.INT64) {
      @Override
      public Long getInt64() {
        return myLong;
      }
    };
  }

  public static CEPLiteral of(BigDecimal myDecimal) {
    return new CEPLiteral(CEPTypeName.DECIMAL) {
      @Override
      public BigDecimal getDecimal() {
        return myDecimal;
      }
    };
  }

  public static CEPLiteral of(Float myFloat) {
    return new CEPLiteral(CEPTypeName.FLOAT) {
      @Override
      public Float getFloat() {
        return myFloat;
      }
    };
  }

  public static CEPLiteral of(Double myDouble) {
    return new CEPLiteral(CEPTypeName.DOUBLE) {
      @Override
      public Double getDouble() {
        return myDouble;
      }
    };
  }

  public static CEPLiteral of(ReadableDateTime myDateTime) {
    return new CEPLiteral(CEPTypeName.DATETIME) {
      @Override
      public ReadableDateTime getDateTime() {
        return myDateTime;
      }
    };
  }

  public static CEPLiteral of(Boolean myBoolean) {
    return new CEPLiteral(CEPTypeName.BOOLEAN) {
      @Override
      public Boolean getBoolean() {
        return myBoolean;
      }
    };
  }

  public static CEPLiteral of(String myString) {
    return new CEPLiteral(CEPTypeName.STRING) {
      @Override
      public String getString() {
        return myString;
      }
    };
  }

  public Byte getByte() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public Short getInt16() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public Integer getInt32() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public Long getInt64() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public BigDecimal getDecimal() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public Float getFloat() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public Double getDouble() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public ReadableDateTime getDateTime() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public Boolean getBoolean() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public String getString() {
    throw new SqlConversionException("the class must be subclassed properly to get the value");
  }

  public CEPTypeName getTypeName() {
    return typeName;
  }
}
