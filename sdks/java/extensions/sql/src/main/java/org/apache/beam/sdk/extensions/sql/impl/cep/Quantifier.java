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

import java.io.Serializable;

/**
 * The {@code Quantifier} class is intended for storing the information of the quantifier for a
 * pattern variable.
 */
public class Quantifier implements Serializable {

  public static final Quantifier NONE = new Quantifier("");
  public static final Quantifier PLUS = new Quantifier("+");
  public static final Quantifier QMARK = new Quantifier("?");
  public static final Quantifier ASTERISK = new Quantifier("*");
  public static final Quantifier PLUS_RELUCTANT = new Quantifier("+?");
  public static final Quantifier ASTERISK_RELUCTANT = new Quantifier("*?");
  public static final Quantifier QMARK_RELUCTANT = new Quantifier("??");

  private final String repr;

  Quantifier(String repr) {
    this.repr = repr;
  }

  @Override
  public String toString() {
    return repr;
  }
}
