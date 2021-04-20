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

import com.google.zetasql.io.grpc.Status;
import com.google.zetasql.io.grpc.StatusRuntimeException;

/**
 * Exception to be thrown by the Beam ZetaSQL planner.
 *
 * <p>Wraps a {@link StatusRuntimeException} containing a GRPC status code.
 */
public class ZetaSqlException extends RuntimeException {

  public ZetaSqlException(StatusRuntimeException cause) {
    super(cause);
  }

  public ZetaSqlException(String message) {
    this(Status.UNIMPLEMENTED.withDescription(message).asRuntimeException());
  }
}
