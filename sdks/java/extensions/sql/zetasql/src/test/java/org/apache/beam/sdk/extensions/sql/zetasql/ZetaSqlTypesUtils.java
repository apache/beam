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
package org.apache.beam.sdk.extensions.sql.zetasql;

import java.math.BigDecimal;
import org.apache.beam.sdk.annotations.Internal;

/** Utils to deal with ZetaSQL type generation. */
@Internal
public class ZetaSqlTypesUtils {

  private ZetaSqlTypesUtils() {}

  /**
   * Create a ZetaSQL NUMERIC value represented as BigDecimal.
   *
   * <p>ZetaSQL NUMERIC type is an exact numeric value with 38 digits of precision and 9 decimal
   * digits of scale.
   *
   * <p>Precision is the number of digits that the number contains.
   *
   * <p>Scale is how many of these digits appear after the decimal point.
   */
  public static BigDecimal bigDecimalAsNumeric(String s) {
    return new BigDecimal(s).setScale(9);
  }
}
