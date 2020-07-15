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
 * For deciding the value type of a {@code CEPLiteral}, {@code CEPTypeName} intends for storing the
 * type for a literal.
 */
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
