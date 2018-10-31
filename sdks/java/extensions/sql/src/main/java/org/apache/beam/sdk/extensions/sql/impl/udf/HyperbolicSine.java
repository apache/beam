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

/**
 * SINH(X)
 *
 * <p>Computes the hyperbolic sine of X. Generates an error if an overflow occurs.
 */
@AutoService(BeamBuiltinFunctionClass.class)
public class HyperbolicSine implements BeamBuiltinFunctionClass {
  private static final String SQL_FUNCTION_NAME = "SINH";

  // TODO: handle overflow
  @UserDefinedFunctionAnnotation(
    funcName = SQL_FUNCTION_NAME,
    parameterArray = {Double.class},
    returnType = Double.class
  )
  public Double sinh(Double o) {
    return Math.sinh(o);
  }
}
