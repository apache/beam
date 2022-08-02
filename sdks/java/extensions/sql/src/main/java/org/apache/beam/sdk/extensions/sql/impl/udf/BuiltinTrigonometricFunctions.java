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
package org.apache.beam.sdk.extensions.sql.impl.udf;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.schemas.Schema;

/** TrigonometricFunctions. */
@AutoService(BeamBuiltinFunctionProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BuiltinTrigonometricFunctions extends BeamBuiltinFunctionProvider {

  /**
   * COSH(X)
   *
   * <p>Computes the hyperbolic cosine of X. Generates an error if an overflow occurs.
   */
  // TODO: handle overflow
  @UDF(
      funcName = "COSH",
      parameterArray = {Schema.TypeName.DOUBLE},
      returnType = Schema.TypeName.DOUBLE)
  public Double cosh(Double o) {
    if (o == null) {
      return null;
    }
    return Math.cosh(o);
  }

  /**
   * SINH(X)
   *
   * <p>Computes the hyperbolic sine of X. Generates an error if an overflow occurs.
   */
  // TODO: handle overflow
  @UDF(
      funcName = "SINH",
      parameterArray = {Schema.TypeName.DOUBLE},
      returnType = Schema.TypeName.DOUBLE)
  public Double sinh(Double o) {
    if (o == null) {
      return null;
    }
    return Math.sinh(o);
  }

  /**
   * TANH(X)
   *
   * <p>Computes hyperbolic tangent of X. Does not fail.
   */
  @UDF(
      funcName = "TANH",
      parameterArray = {Schema.TypeName.DOUBLE},
      returnType = Schema.TypeName.DOUBLE)
  public Double tanh(Double o) {
    if (o == null) {
      return null;
    }
    return Math.tanh(o);
  }
}
