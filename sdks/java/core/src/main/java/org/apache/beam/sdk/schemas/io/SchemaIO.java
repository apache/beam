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
package org.apache.beam.sdk.schemas.io;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * An abstraction to create schema capable and aware IOs. The interface is intended to be used in
 * conjunction with the interface {@link SchemaIOProvider}.
 *
 * <p>The interfaces can be implemented to make IOs available in other SDKs in addition to Beam SQL.
 *
 * <p><b>Internal only:</b> This interface is actively being worked on and it will likely change as
 * we provide implementations for more standard Beam IOs. We provide no backwards compatibility
 * guarantees and it should not be implemented outside of the Beam repository.
 *
 * <p>For a detailed walkthrough of implementation, see <a
 * href="s.apache.org/schemaio-development-guide">s.apache.org/schemaio-development-guide </a>
 */
@Internal
@Experimental(Kind.SCHEMAS)
public interface SchemaIO {
  /** Returns the schema of the data. */
  Schema schema();

  /** Returns a schema aware reader. */
  PTransform<PBegin, PCollection<Row>> buildReader();

  /** Returns a schema aware writer. */
  PTransform<PCollection<Row>, ? extends POutput> buildWriter();
}
