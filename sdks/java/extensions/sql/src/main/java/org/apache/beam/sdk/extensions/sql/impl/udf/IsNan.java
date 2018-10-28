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

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;

/**
 * IS_NAN(X)
 *
 * <p>Returns TRUE if the value is a NaN value. Returns NULL for NULL inputs. input: Float, Double
 *
 * <p>Output: Boolean
 */
public class IsNan implements BeamSqlUdf {
  public static final String FUNCTION_NAME = "IS_NAN";

  @VisibleForTesting
  static final String ERROR_MSG = FUNCTION_NAME + " only accepts FLOAT or DOUBLE type.";

  public static boolean eval(Object value) throws Exception {
    if (value instanceof Float) {
      return Float.isNaN((Float) value);
    } else if (value instanceof Double) {
      return Double.isNaN((Double) value);
    }

    throw new Exception(ERROR_MSG);
  }
}
