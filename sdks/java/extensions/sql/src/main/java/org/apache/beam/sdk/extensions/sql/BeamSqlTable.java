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

package org.apache.beam.sdk.extensions.sql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * This interface defines a Beam Sql Table.
 */
public interface BeamSqlTable {
  /**
   * In Beam SQL, there's no difference between a batch query and a streaming
   * query. {@link BeamIOType} is used to validate the sources.
   */
  BeamIOType getSourceType();

  /**
   * create a {@code PCollection<BeamSqlRow>} from source.
   *
   */
  PCollection<Row> buildIOReader(Pipeline pipeline);

  /**
   * create a {@code IO.write()} instance to write to target.
   *
   */
   PTransform<? super PCollection<Row>, POutput> buildIOWriter();

  /**
   * Get the schema info of the table.
   */
   Schema getSchema();
}
