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
package org.apache.beam.sdk.extensions.sql.meta.catalog;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * A stored procedure that can be invoked with the SQL {@code CALL} statement:
 *
 * <pre>{@code
 * CALL [catalog_name.][system.]procedure_name(arg1, arg2, ...);
 * CALL [catalog_name.][system.]procedure_name(param2 => arg2, param1 => arg1, ...);
 * }</pre>
 *
 * <p>Procedures are provided by a {@link Catalog} via {@link Catalog#loadProcedure(String)}.
 * Arguments may be passed by position or by name (but not both in the same call), and are validated
 * and bound against {@link #parameters()} before {@link #execute(Row)} is invoked.
 */
@Internal
public interface Procedure {

  /** The name of this procedure, in {@code lower_snake_case} (e.g. {@code "add_files"}). */
  String name();

  /**
   * Declares this procedure's parameters.
   *
   * <p>Field order defines the positional-argument order. Fields that are non-nullable are required
   * arguments; nullable fields are optional and default to null when omitted.
   */
  Schema parameters();

  /**
   * Runs the procedure. {@code args} is a {@link Row} over {@link #parameters()} holding the bound
   * argument values, with omitted optional arguments set to null.
   */
  void execute(Row args);
}
