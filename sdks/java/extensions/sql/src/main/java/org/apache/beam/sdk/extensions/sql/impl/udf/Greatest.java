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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;

/**
 * GREATEST(X1,...,XN)
 *
 * <p>Returns NULL if any of the inputs is NULL. Otherwise, returns NaN if any of the inputs is NaN.
 * Otherwise, returns the largest value among X1 to XN according to the less than comparison.
 */
public class Greatest implements BeamSqlUdf {
  public static final String FUNCTION_NAME = "GREATEST";

  static final String ERROR_MSG = FUNCTION_NAME + " does not accept empty list.";

  public static Object eval(List<Object> value) throws Exception {
    if (value.isEmpty()) {
      throw new Exception(ERROR_MSG);
    }

    if (UDFUtils.hasNull(value)) {
      return null;
    }

    Object ret = UDFUtils.hasNaN(value);
    if (ret != null) {
      return ret;
    }

    return value.get(0);
  }
}
