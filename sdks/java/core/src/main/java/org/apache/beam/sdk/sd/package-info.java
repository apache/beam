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
/**
 * {@link org.apache.beam.sdk.sd.BeamRow}, self-described with
 * {@link org.apache.beam.sdk.sd.BeamRowType}, and encoded/decoded with
 * {@link org.apache.beam.sdk.sd.BeamRowCoder} is the foundation of structure data process in Beam.
 *
 * <p>Similar as the <em>row</em> concept in database, {@link org.apache.beam.sdk.sd.BeamRow}
 * represents one row element in a {@link org.apache.beam.sdk.values.PCollection<BeamRow>}.
 * Limited SQL types are supported now, visit
 * <a href="https://beam.apache.org/blog/2017/07/21/sql-dsl.html#data-type">data types</a>
 * for more details.
 */
package org.apache.beam.sdk.sd;
